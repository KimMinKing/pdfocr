# -*- coding: utf-8 -*-
"""
멀티 심볼 실시간 신호 모니터 — BTCUSDT / ETHUSDT / XRPUSDT
abb_lab1 전략 (A+D 풀트레일) 기반
"""

import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import Optional, List

import numpy as np

from abb_lab1 import (
    # 클래스
    Calibrator, StatAnalyzer, StatRecord, MomentumTracker, PerformanceFeedback,
    MeanReversionTracker, QpulseSetupTracker, QPulseArmedReversionTracker,
    SplitPosition, Performance, TelegramBot,
    # 함수
    fetch_klines, fetch_last_price, classify_trend, combine_timeframes,
    calc_sync_score, calc_qpulse_multitf_score, check_range_signal,
    decide_entry, decide_exit, apply_slippage, fee, fmt_price, fmt_time,
    # 상수
    TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, CATEGORY, TIMEFRAMES, CANDLES_LIMIT,
    CACHE_SECONDS, START_BALANCE, LEVERAGE, MAX_CONCURRENT_SIGNALS, MAX_RETRIES,
    REFRESH_SEC, RANGE_RSI_OVERSOLD, RANGE_RSI_OVERBOUGHT,
)

# ============================================================
# 설정
# ============================================================
SYMBOLS = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]


# ============================================================
# 심볼별 상태
# ============================================================
@dataclass
class SymbolState:
    symbol: str
    cal: Calibrator = field(default_factory=Calibrator)
    perf: Performance = field(default_factory=Performance)
    stat: StatAnalyzer = field(default_factory=StatAnalyzer)
    momentum: MomentumTracker = field(default_factory=MomentumTracker)
    feedback: PerformanceFeedback = field(default_factory=PerformanceFeedback)
    reversion: MeanReversionTracker = field(default_factory=MeanReversionTracker)
    qpulse_tracker: QpulseSetupTracker = field(default_factory=QpulseSetupTracker)
    qp_armed_tracker: QPulseArmedReversionTracker = field(default_factory=QPulseArmedReversionTracker)
    positions: List[SplitPosition] = field(default_factory=list)
    tf_cache: dict = field(default_factory=dict)
    last_fetch_time: dict = field(default_factory=dict)
    signal_counter: int = 0
    last_exit_time: Optional[datetime] = None
    last_exit_side: Optional[str] = None
    prev_tf_data: Optional[dict] = None
    atr_history: deque = field(default_factory=lambda: deque(maxlen=50))
    last_alerted_side: Optional[str] = None


# ============================================================
# 청산 알림
# ============================================================
def _notify_exit(sym: SymbolState, pos: SplitPosition, tg: TelegramBot,
                 exit_px: float, pnl: float, reason: str):
    pnl_pct = pnl / START_BALANCE * 100
    emoji = "O" if pnl >= 0 else "X"
    msg = (
        f"{emoji} [{sym.symbol}] 청산 #{pos.signal_id} {pos.side} | {pos.strategy}\n"
        f"청산가: {fmt_price(exit_px)}  (진입: {fmt_price(pos.avg_entry_price)})\n"
        f"사유: {reason}\n"
        f"손익: ${pnl:+.2f} ({pnl_pct:+.2f}%)"
    )
    print(f"[{fmt_time()}] {msg.replace(chr(10), ' | ')}")
    tg.send(msg)


# ============================================================
# 단일 심볼 처리
# ============================================================
def process_symbol(sym: SymbolState, tg: TelegramBot, now: datetime):
    # === 데이터 수집 ===
    tf = {}
    for name, interval in TIMEFRAMES.items():
        cache_valid = (
            name in sym.tf_cache and name in sym.last_fetch_time and
            (now - sym.last_fetch_time[name]).total_seconds() < CACHE_SECONDS[name]
        )
        if cache_valid:
            tf[name] = sym.tf_cache[name]
        else:
            df = fetch_klines(sym.symbol, interval, CANDLES_LIMIT, CATEGORY)
            tf[name] = classify_trend(df)
            sym.tf_cache[name] = tf[name]
            sym.last_fetch_time[name] = now
            time.sleep(0.2)

    base = combine_timeframes(tf)
    p_up_base = base["p_up_base"]
    p_up = sym.cal.calibrate(p_up_base)
    is_ranging = base["is_ranging"]
    sym.cal.update_on_15m_close(tf["15m"]["time"], tf["15m"]["close"], p_up_base)

    last_price = fetch_last_price(sym.symbol, CATEGORY)
    atr15 = tf["15m"]["atr14"]
    sym.atr_history.append(atr15)
    avg_atr = float(np.mean(sym.atr_history))

    # === 모멘텀 ===
    delta_p, speed, accel = sym.momentum.update(p_up)

    # === 멀티TF 동조 ===
    sync_score = calc_sync_score(tf, sym.prev_tf_data)
    sym.prev_tf_data = {k: {"score": v["score"]} for k, v in tf.items()}

    # === 통계 레코드 ===
    rec = StatRecord(
        timestamp=now, p_up=p_up, delta_p=delta_p,
        delta_p_speed=speed, delta_p_accel=accel,
        price=last_price, sync_score=sync_score,
    )
    sym.stat.add_record(rec)
    sym.stat.fill_future_prices(last_price, now)
    sym.stat.update_mfe_mae_realtime(last_price)

    range_signal = (
        check_range_signal(tf, RANGE_RSI_OVERSOLD, RANGE_RSI_OVERBOUGHT)
        if is_ranging else {"action": None}
    )

    # Q-Pulse BB 신호 (15m 기준)
    tf15 = tf["15m"]
    qpulse_signal = sym.qpulse_tracker.update(
        ema19=tf15["ema19"], ema40=tf15["ema40"],
        ema19_prev=tf15["ema19_prev"], ema40_prev=tf15["ema40_prev"],
        qpulse=tf15["qpulse"], close=tf15["close"],
        bb_upper=tf15["bb_upper"], bb_lower=tf15["bb_lower"],
        bb_middle=tf15["bb_middle"],
    )

    # Q-Pulse Armed 회귀 신호
    qpulse_mtf_score = calc_qpulse_multitf_score(tf)
    qp_armed_signal = sym.qp_armed_tracker.update(
        qpulse_score=qpulse_mtf_score, price=last_price, delta_p=delta_p, now=now,
    )

    # ============================================
    # 진입 판단
    # ============================================
    decision = decide_entry(
        p_up=p_up, delta_p=delta_p, accel=accel,
        sync_score=sync_score, is_ranging=is_ranging,
        range_signal=range_signal, stat_analyzer=sym.stat,
        perf=sym.perf, feedback=sym.feedback,
        reversion_tracker=sym.reversion,
        qpulse_tracker=sym.qpulse_tracker,
        qpulse_signal=qpulse_signal,
        qp_armed_tracker=sym.qp_armed_tracker,
        qp_armed_signal=qp_armed_signal,
        now=now, last_price=last_price, atr15=atr15, avg_atr=avg_atr, tf=tf,
        last_exit_time=sym.last_exit_time, last_exit_side=sym.last_exit_side,
    )

    if decision.should_enter and len(sym.positions) < MAX_CONCURRENT_SIGNALS:
        sym.signal_counter += 1
        sid = sym.signal_counter

        entry_px = apply_slippage(last_price, decision.side, is_entry=True)
        available = sym.perf.balance * decision.position_ratio
        notional = available * LEVERAGE
        qty = notional / entry_px
        fees = fee(notional)
        sym.perf.balance -= fees

        new_pos = SplitPosition(side=decision.side, strategy=decision.strategy, signal_id=sid)
        new_pos.add_entry(entry_px, qty, now, fees)
        new_pos.initial_stop = decision.stop_price
        if decision.is_qpulse_bb:
            new_pos.qpulse_bb_middle = 0.0   # [풀트레일] TP 비활성화
            new_pos._tightened = False
        if decision.is_reversion:
            new_pos.reversion_deviation_peak_price = decision.reversion_deviation_peak_price
            new_pos.reversion_tp1_price = decision.reversion_tp1_price
            new_pos.reversion_tp2_price = decision.reversion_tp2_price
        if decision.is_qp_armed_rev:
            new_pos.qp_armed_peak_price = decision.qp_armed_peak_price
            new_pos.qp_armed_tp1_price = decision.qp_armed_tp1_price
            new_pos.qp_armed_tp2_price = decision.qp_armed_tp2_price
        new_pos.trailing.init(entry_px, decision.side, decision.stop_price, atr15)
        sym.positions.append(new_pos)

        _tp_line = ""
        if decision.is_reversion:
            _tp_line = (f"\nTP1: {fmt_price(decision.reversion_tp1_price)}"
                        f"  TP2: {fmt_price(decision.reversion_tp2_price)}")
        elif decision.is_qp_armed_rev:
            _tp_line = (f"\n극값: {fmt_price(decision.qp_armed_peak_price)}"
                        f"  TP1: {fmt_price(decision.qp_armed_tp1_price)}"
                        f"  TP2: {fmt_price(decision.qp_armed_tp2_price)}")

        msg = (
            f"[{sym.symbol}] 진입 #{sid} {decision.side} | {decision.strategy}\n"
            f"가격: {fmt_price(entry_px)}  손절: {fmt_price(decision.stop_price)}"
            f"{_tp_line}\n"
            f"신뢰도: {decision.confidence_tier}  포지션: {len(sym.positions)}/{MAX_CONCURRENT_SIGNALS}"
        )
        print(f"[{fmt_time()}] {msg.replace(chr(10), ' | ')}")
        tg.send(msg)
        sym.last_alerted_side = decision.side

    elif decision.should_enter and len(sym.positions) >= MAX_CONCURRENT_SIGNALS:
        print(f"[{fmt_time()}] [{sym.symbol}] 신호 스킵 — 최대 포지션({MAX_CONCURRENT_SIGNALS}) 도달 "
              f"| {decision.side} @ {fmt_price(last_price)} | {decision.strategy}")

    # ============================================
    # 포지션 관리 (청산 체크)
    # ============================================
    for pos in sym.positions[:]:
        pos.tick(last_price)

        # [풀트레일] Option D: 5m EMA 역전 조기청산 (15분 이상 보유 후)
        if pos.strategy == "QPULSE_BB" and pos.exit_phase == 0:
            hold_secs = (now - pos.entry_start_time).total_seconds() if pos.entry_start_time else 0
            if hold_secs >= 15 * 60:
                ema19_5m = tf["5m"].get("ema19", 0.0)
                ema40_5m = tf["5m"].get("ema40", 0.0)
                ema_against = (
                    (pos.side == "LONG"  and ema19_5m > 0 and ema19_5m < ema40_5m) or
                    (pos.side == "SHORT" and ema19_5m > 0 and ema19_5m > ema40_5m)
                )
                if ema_against:
                    ex_px = apply_slippage(last_price, pos.side, is_entry=False)
                    ex_fees = fee(pos.remaining_qty * ex_px)
                    sym.perf.balance -= ex_fees
                    pnl = pos.partial_exit(pos.remaining_qty, ex_px, ex_fees)
                    sym.perf.balance += pnl
                    sym.positions.remove(pos)
                    sym.last_exit_time = now
                    sym.last_exit_side = pos.side
                    sym.last_alerted_side = None
                    _notify_exit(sym, pos, tg, ex_px, pnl, "5mEMA역전조기청산")
                    continue

        exit_reason, exit_ratio = decide_exit(
            pos=pos, p_up=p_up, delta_p=delta_p,
            accel=accel, speed=speed, sync_score=sync_score,
            last_price=last_price,
            is_ranging=is_ranging, range_signal=range_signal,
            stat_analyzer=sym.stat, now=now, atr15=atr15,
        )

        # [풀트레일] 부분익절 스킵
        if (pos.strategy == "QPULSE_BB" and exit_reason is not None
                and "익절" in exit_reason and exit_ratio < 0.99):
            exit_reason = None

        if exit_reason is None:
            continue

        exit_qty = pos.remaining_qty * exit_ratio
        exit_px = apply_slippage(last_price, pos.side, is_entry=False)
        exit_fees = fee(exit_qty * exit_px)
        sym.perf.balance -= exit_fees
        pnl = pos.partial_exit(exit_qty, exit_px, exit_fees)
        sym.perf.balance += pnl

        is_full_exit = (pos.remaining_qty < 0.0001) or exit_ratio >= 0.99
        if is_full_exit:
            sym.last_exit_time = now
            sym.last_exit_side = pos.side
            sym.perf.trades += 1
            if pnl < 0:
                sym.perf.consecutive_losses += 1
                sym.perf.stoplosses += 1
            else:
                sym.perf.consecutive_losses = 0
            sym.positions.remove(pos)
            sym.last_alerted_side = None
            _notify_exit(sym, pos, tg, exit_px, pnl, exit_reason)
        else:
            # 부분청산
            pnl_pct = pnl / START_BALANCE * 100
            msg = (
                f"[{sym.symbol}] 부분청산 #{pos.signal_id} {exit_ratio:.0%} {pos.side}\n"
                f"@ {fmt_price(exit_px)}  PnL: ${pnl:+.2f} ({pnl_pct:+.2f}%)\n"
                f"사유: {exit_reason}"
            )
            print(f"[{fmt_time()}] {msg.replace(chr(10), ' | ')}")
            tg.send(msg)


# ============================================================
# 메인
# ============================================================
def main():
    print("=" * 60)
    print("  멀티 심볼 신호 모니터")
    print(f"  심볼: {' / '.join(SYMBOLS)}")
    print("  전략: A+D 풀트레일 (QPULSE_BB + 회귀 전략)")
    print(f"  갱신주기: {REFRESH_SEC}초")
    print("=" * 60)

    tg = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    states = [SymbolState(symbol=s) for s in SYMBOLS]
    retry_count = 0

    tg.send(f"멀티 신호 모니터 시작\n심볼: {', '.join(SYMBOLS)}\n전략: A+D 풀트레일")

    while True:
        try:
            now = datetime.now(timezone.utc)
            for sym in states:
                process_symbol(sym, tg, now)
            retry_count = 0
            time.sleep(REFRESH_SEC)

        except Exception as e:
            retry_count += 1
            print(f"[{fmt_time()}] 오류 ({retry_count}/{MAX_RETRIES}): {type(e).__name__}: {e}")
            print(traceback.format_exc())
            if retry_count >= MAX_RETRIES:
                tg.send(f"멀티 모니터 {MAX_RETRIES}회 연속 실패. 종료.\n오류: {e}")
                break
            time.sleep(REFRESH_SEC)


if __name__ == "__main__":
    main()
