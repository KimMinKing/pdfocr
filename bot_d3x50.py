#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
D3x50 실매매 봇 — BTC / ETH / XRP  |  Bitget USDT 선물  |  50x 레버리지

  신호 소스 : Bybit 공개 캔들 데이터  (fetch_klines — API 키 불필요)
  실행 거래소: Bitget USDT-FUTURES
  전략      : QPULSE_BB  D3 필터  (VOL 0.90x / BB 45%)
  방향      : 백테스트와 동일한 추세추종 방향 (_flip 적용)

  포지션 규칙
    - 신호 발생 시 Bitget USDT 가용잔고의 50% × 50x = 노셔널
    - 전체 3개 심볼 중 최대 2개 동시 보유
    - 3번째 신호는 무시 (이미 2개 포지션 활성 시)

  실행:
    python bot_d3x50.py

  주의:
    - abb_lab1_d3x50.py 에 설정된 TELEGRAM_TOKEN/CHAT_ID로 알림 전송
    - DRY_RUN = True 이면 주문 API 호출 없이 로그만 출력
"""

import hmac, hashlib, base64, time, json, math, traceback
import requests
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import Optional, List

import numpy as np

import abb_lab1_d3x50 as S

# =============================================================================
# Bitget API 인증 정보  (bitget_api_test.py 에서 확인된 키)
# =============================================================================
BITGET_KEY        = "bg_16af1ea44d4ff168872230a4ecb6a95f"
BITGET_SECRET     = "b042fd219aaa04a69ef122a83d89ca9419cacd6ee5b832fbd5f37e3214d2b74f"
BITGET_PASSPHRASE = "d3x50trade"
BITGET_BASE       = "https://api.bitget.com"
PRODUCT_TYPE      = "USDT-FUTURES"
MARGIN_COIN       = "USDT"

# =============================================================================
# 봇 설정
# =============================================================================
SYMBOLS        = ["BTCUSDT", "ETHUSDT", "XRPUSDT"]
TARGET_LEVERAGE = 50
MAX_GLOBAL_POS  = 2       # 전체 심볼 합산 최대 동시 포지션
POS_RATIO       = 0.50    # 신호당 가용잔고 비율 (50%)
DRY_RUN         = False   # True = 주문 실행 안 함 (테스트 모드)

# 심볼별 수량 소수점 자리 (Bitget 최소 주문 단위 기준)
QTY_DEC   = {"BTCUSDT": 3,   "ETHUSDT": 2,    "XRPUSDT": 0}
# 심볼별 가격 틱 사이즈 (5틱 지정가 진입용)
TICK_SIZE = {"BTCUSDT": 0.1, "ETHUSDT": 0.01, "XRPUSDT": 0.0001}
# 심볼별 가격 소수점 자리
PRICE_DEC = {"BTCUSDT": 1,   "ETHUSDT": 2,    "XRPUSDT": 4}

REFRESH_SEC = 20
MAX_RETRIES = 5


# =============================================================================
# Bitget API 래퍼
# =============================================================================
class BitgetTrader:
    def __init__(self):
        self.key        = BITGET_KEY
        self.secret     = BITGET_SECRET
        self.passphrase = BITGET_PASSPHRASE

    def _sign(self, ts: str, method: str, path: str, body: str = "") -> str:
        msg = ts + method.upper() + path + body
        mac = hmac.new(self.secret.encode(), msg.encode(), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode()

    def _headers(self, method: str, path: str, body: str = "") -> dict:
        ts = str(int(time.time() * 1000))
        return {
            "ACCESS-KEY":        self.key,
            "ACCESS-SIGN":       self._sign(ts, method, path, body),
            "ACCESS-TIMESTAMP":  ts,
            "ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type":      "application/json",
            "locale":            "en-US",
        }

    def _get(self, path: str, params: dict = None) -> dict:
        query = ("?" + "&".join(f"{k}={v}" for k, v in params.items())) if params else ""
        r = requests.get(BITGET_BASE + path + query,
                         headers=self._headers("GET", path + query), timeout=10)
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        body_str = json.dumps(body)
        r = requests.post(BITGET_BASE + path, headers=self._headers("POST", path, body_str),
                          data=body_str, timeout=10)
        return r.json()

    # ── 잔고 ──────────────────────────────────────────────────
    def get_usdt_balance(self) -> float:
        """가용 USDT 잔고 반환"""
        r = self._get("/api/v2/mix/account/accounts", {"productType": PRODUCT_TYPE})
        if r.get("code") != "00000":
            raise RuntimeError(f"잔고 조회 실패: {r.get('msg')}")
        for acc in r.get("data", []):
            if acc.get("marginCoin") == "USDT":
                return float(acc.get("available", 0))
        return 0.0

    # ── 초기화 ────────────────────────────────────────────────
    def init_symbol(self, symbol: str):
        """레버리지 50x 설정 (LONG/SHORT 모두)"""
        for side in ("long", "short"):
            r = self._post("/api/v2/mix/account/set-leverage", {
                "symbol":      symbol,
                "productType": PRODUCT_TYPE,
                "marginCoin":  MARGIN_COIN,
                "leverage":    str(TARGET_LEVERAGE),
                "holdSide":    side,
            })
            if r.get("code") != "00000":
                print(f"  ⚠️  {symbol} {side} 레버리지 설정: {r.get('msg')}")

    # ── 진입 주문 (지정가 5틱) ────────────────────────────────
    def place_entry(self, symbol: str, side: str, qty: float,
                    ref_price: float) -> Optional[str]:
        """지정가 진입 주문 (5틱 유리한 방향).
        GTC 주문 후 3초 대기 → 체결 확인 → 미체결 시 취소.
        반환: orderId(체결됨) 또는 None(미체결/실패)
        """
        bitget_side = "buy" if side == "LONG" else "sell"
        dec  = QTY_DEC.get(symbol, 3)
        pdec = PRICE_DEC.get(symbol, 2)
        tick = TICK_SIZE.get(symbol, 0.01)
        size = round(qty, dec)
        if size <= 0:
            print(f"  ⚠️  {symbol} 주문 수량 0 이하 — 스킵")
            return None

        # LONG: 현재가보다 5틱 낮게 / SHORT: 5틱 높게
        offset = tick * 5
        limit_price = round(
            ref_price - offset if side == "LONG" else ref_price + offset,
            pdec,
        )

        if DRY_RUN:
            print(f"  [DRY] {symbol} {side} {size} 지정가 @ {limit_price:.{pdec}f}")
            return "DRY_ENTRY"

        r = self._post("/api/v2/mix/order/place-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "size":        str(size),
            "side":        bitget_side,
            "tradeSide":   "open",
            "orderType":   "limit",
            "price":       f"{limit_price:.{pdec}f}",
            "force":       "gtc",
        })
        if r.get("code") != "00000":
            print(f"  ❌ {symbol} 진입 실패: {r.get('msg')}")
            return None

        order_id = r.get("data", {}).get("orderId")
        # 3초 대기 후 체결 여부 확인
        time.sleep(3)
        if self._is_order_filled(symbol, order_id):
            print(f"  ✅ {symbol} 지정가 체결 @ {limit_price:.{pdec}f}")
            return order_id
        # 미체결 → 취소
        self._cancel_order(symbol, order_id)
        print(f"  ⚠️  {symbol} 지정가 미체결 → 취소 (ref={ref_price:.{pdec}f})")
        return None

    def _is_order_filled(self, symbol: str, order_id: str) -> bool:
        r = self._get("/api/v2/mix/order/detail", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "orderId":     order_id,
        })
        if r.get("code") != "00000":
            return False
        return r.get("data", {}).get("state") in ("filled", "full_fill")

    def _cancel_order(self, symbol: str, order_id: str) -> bool:
        r = self._post("/api/v2/mix/order/cancel-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "orderId":     order_id,
        })
        return r.get("code") == "00000"

    # ── 손절 (Plan Order) ─────────────────────────────────────
    def place_stop_loss(self, symbol: str, hold_side: str,
                        qty: float, stop_price: float) -> Optional[str]:
        """포지션 손절 트리거 주문. 반환: planOrderId 또는 None"""
        dec = QTY_DEC.get(symbol, 3)
        size = round(qty, dec)
        price_dec = 2 if "BTC" in symbol else (4 if "XRP" in symbol else 2)

        if DRY_RUN:
            print(f"  [DRY] {symbol} 손절 설정 @ {stop_price:.{price_dec}f}")
            return "DRY_STOP"

        r = self._post("/api/v2/mix/order/place-tpsl-order", {
            "symbol":       symbol,
            "productType":  PRODUCT_TYPE,
            "marginCoin":   MARGIN_COIN,
            "planType":     "loss_plan",
            "triggerPrice": f"{stop_price:.{price_dec}f}",
            "triggerType":  "mark_price",
            "size":         str(size),
            "holdSide":     hold_side.lower(),
            "orderType":    "market",
        })
        if r.get("code") != "00000":
            print(f"  ⚠️  {symbol} 손절 주문 실패: {r.get('msg')}")
            return None
        return r.get("data", {}).get("orderId")

    # ── 손절 취소 ─────────────────────────────────────────────
    def cancel_stop_loss(self, symbol: str, plan_order_id: str) -> bool:
        if not plan_order_id or plan_order_id.startswith("DRY"):
            return True
        r = self._post("/api/v2/mix/order/cancel-plan-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "orderId":     plan_order_id,
        })
        return r.get("code") == "00000"

    # ── 청산 주문 ─────────────────────────────────────────────
    def close_position(self, symbol: str, side: str, qty: float) -> bool:
        """시장가 청산. side = 보유 포지션 방향 (LONG/SHORT)"""
        close_side = "sell" if side == "LONG" else "buy"
        dec = QTY_DEC.get(symbol, 3)
        size = round(qty, dec)

        if DRY_RUN:
            print(f"  [DRY] {symbol} {side} {size} 청산")
            return True

        r = self._post("/api/v2/mix/order/place-order", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
            "size":        str(size),
            "side":        close_side,
            "tradeSide":   "close",
            "orderType":   "market",
            "force":       "gtc",
        })
        ok = r.get("code") == "00000"
        if not ok:
            print(f"  ❌ {symbol} 청산 실패: {r.get('msg')}")
        return ok

    # ── 포지션 조회 ───────────────────────────────────────────
    def get_position(self, symbol: str) -> Optional[dict]:
        """거래소 실제 포지션 반환. 없으면 None"""
        r = self._get("/api/v2/mix/position/single-position", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
            "marginCoin":  MARGIN_COIN,
        })
        if r.get("code") != "00000":
            return None
        for p in r.get("data", []):
            if float(p.get("total", 0)) > 0:
                return p
        return None

    # ── 현재가 ────────────────────────────────────────────────
    def get_mark_price(self, symbol: str) -> float:
        r = self._get("/api/v2/mix/market/ticker", {
            "symbol":      symbol,
            "productType": PRODUCT_TYPE,
        })
        if r.get("code") != "00000":
            return 0.0
        data = r.get("data", [{}])
        lst = data[0] if isinstance(data, list) else data
        return float(lst.get("markPrice") or lst.get("lastPr") or 0)


# =============================================================================
# 전역 포지션 카운터
# =============================================================================
class GlobalState:
    def __init__(self):
        self.active: set = set()   # 현재 포지션 보유 중인 심볼

    def count(self) -> int:
        return len(self.active)

    def add(self, symbol: str):
        self.active.add(symbol)

    def remove(self, symbol: str):
        self.active.discard(symbol)

    def can_enter(self, symbol: str) -> bool:
        return symbol in self.active or self.count() < MAX_GLOBAL_POS


# =============================================================================
# 심볼 상태 (signal_multi.py 확장)
# =============================================================================
@dataclass
class BotSymbolState:
    symbol: str
    cal:            S.Calibrator                 = field(default_factory=S.Calibrator)
    perf:           S.Performance                = field(default_factory=S.Performance)
    stat:           S.StatAnalyzer               = field(default_factory=S.StatAnalyzer)
    momentum:       S.MomentumTracker            = field(default_factory=S.MomentumTracker)
    feedback:       S.PerformanceFeedback        = field(default_factory=S.PerformanceFeedback)
    reversion:      S.MeanReversionTracker       = field(default_factory=S.MeanReversionTracker)
    qpulse_tracker: S.QpulseSetupTracker         = field(default_factory=S.QpulseSetupTracker)
    qp_armed_tracker: S.QPulseArmedReversionTracker = field(
        default_factory=S.QPulseArmedReversionTracker)
    positions:      List[S.SplitPosition]        = field(default_factory=list)
    tf_cache:       dict                         = field(default_factory=dict)
    last_fetch_time: dict                        = field(default_factory=dict)
    signal_counter: int                          = 0
    last_exit_time: Optional[datetime]           = None
    last_exit_side: Optional[str]                = None
    prev_tf_data:   Optional[dict]               = None
    atr_history:    deque = field(default_factory=lambda: deque(maxlen=50))
    # 실매매 전용
    plan_order_id:  Optional[str]                = None   # 거래소 손절 주문 ID
    live_qty:       float                        = 0.0    # 거래소 실제 보유 수량


# =============================================================================
# 방향 반전 (백테스트와 동일한 추세추종 방향)
# =============================================================================
def _flip(decision, entry_price: float):
    """백테스트(_flip)와 동일: QPULSE_BB 방향 반전 → 추세추종"""
    decision.side = "SHORT" if decision.side == "LONG" else "LONG"
    def mirror(p):
        return 2.0 * entry_price - p if p and p != 0 else p
    decision.stop_price = mirror(decision.stop_price)
    if decision.is_qpulse_bb:
        decision.qpulse_bb_middle = mirror(decision.qpulse_bb_middle or entry_price)
    return decision


# =============================================================================
# 텔레그램 공통 포맷
# =============================================================================
def _fmt(symbol: str, side: str, price: float, stop: float,
         qty: float, pnl: Optional[float], reason: Optional[str], tg) -> None:
    if pnl is not None:
        emoji = "✅" if pnl >= 0 else "❌"
        msg = (f"{emoji} [{symbol}] 청산 {side}\n"
               f"가격: {S.fmt_price(price)}  PnL: ${pnl:+.2f}\n"
               f"사유: {reason}")
    else:
        mode = "[DRY] " if DRY_RUN else ""
        msg = (f"📌 {mode}[{symbol}] 진입 {side}\n"
               f"가격: {S.fmt_price(price)}  손절: {S.fmt_price(stop)}\n"
               f"수량: {qty}")
    print(f"[{S.fmt_time()}] {msg.replace(chr(10), ' | ')}")
    tg.send(msg)


# =============================================================================
# 단일 심볼 처리
# =============================================================================
def process_symbol(sym: BotSymbolState, trader: BitgetTrader,
                   gs: GlobalState, tg, now: datetime):
    # ── 데이터 수집 ──────────────────────────────────────────
    tf = {}
    for name, interval in S.TIMEFRAMES.items():
        cache_ok = (
            name in sym.tf_cache and name in sym.last_fetch_time and
            (now - sym.last_fetch_time[name]).total_seconds() < S.CACHE_SECONDS[name]
        )
        if cache_ok:
            tf[name] = sym.tf_cache[name]
        else:
            df = S.fetch_klines(sym.symbol, interval, S.CANDLES_LIMIT, S.CATEGORY)
            tf[name] = S.classify_trend(df)
            sym.tf_cache[name] = tf[name]
            sym.last_fetch_time[name] = now
            time.sleep(0.15)

    base   = S.combine_timeframes(tf)
    p_up   = sym.cal.calibrate(base["p_up_base"])
    is_rng = base["is_ranging"]
    sym.cal.update_on_15m_close(tf["15m"]["time"], tf["15m"]["close"], base["p_up_base"])

    last_price = S.fetch_last_price(sym.symbol, S.CATEGORY)
    atr15      = tf["15m"]["atr14"]
    sym.atr_history.append(atr15)
    avg_atr = float(np.mean(sym.atr_history))

    delta_p, speed, accel = sym.momentum.update(p_up)
    sync_score = S.calc_sync_score(tf, sym.prev_tf_data)
    sym.prev_tf_data = {k: {"score": v["score"]} for k, v in tf.items()}

    sym.stat.add_record(S.StatRecord(
        timestamp=now, p_up=p_up, delta_p=delta_p,
        delta_p_speed=speed, delta_p_accel=accel,
        price=last_price, sync_score=sync_score,
    ))
    sym.stat.fill_future_prices(last_price, now)
    sym.stat.update_mfe_mae_realtime(last_price)

    range_signal = (S.check_range_signal(tf, S.RANGE_RSI_OVERSOLD, S.RANGE_RSI_OVERBOUGHT)
                    if is_rng else {"action": None})

    tf15 = tf["15m"]
    qpulse_signal = sym.qpulse_tracker.update(
        ema19=tf15["ema19"], ema40=tf15["ema40"],
        ema19_prev=tf15["ema19_prev"], ema40_prev=tf15["ema40_prev"],
        qpulse=tf15["qpulse"], close=tf15["close"],
        bb_upper=tf15["bb_upper"], bb_lower=tf15["bb_lower"],
        bb_middle=tf15["bb_middle"],
    )
    qp_armed_signal = sym.qp_armed_tracker.update(
        qpulse_score=S.calc_qpulse_multitf_score(tf),
        price=last_price, delta_p=delta_p, now=now,
    )

    # ── 포지션 관리 ──────────────────────────────────────────
    for pos in sym.positions[:]:
        pos.tick(last_price)

        # 5m EMA 조기청산 (15분+ 보유 후)
        if pos.strategy == "QPULSE_BB" and pos.exit_phase == 0:
            hold_secs = (now - pos.entry_start_time).total_seconds() if pos.entry_start_time else 0
            if hold_secs >= 15 * 60:
                e19_5m = tf["5m"].get("ema19", 0.0)
                e40_5m = tf["5m"].get("ema40", 0.0)
                ema_against = (
                    (pos.side == "LONG"  and e19_5m > 0 and e19_5m < e40_5m) or
                    (pos.side == "SHORT" and e19_5m > 0 and e19_5m > e40_5m)
                )
                if ema_against:
                    _execute_exit(sym, pos, trader, gs, tg, last_price, "5mEMA역전조기청산", now)
                    continue

        exit_reason, exit_ratio = S.decide_exit(
            pos=pos, p_up=p_up, delta_p=delta_p,
            accel=accel, speed=speed, sync_score=sync_score,
            last_price=last_price, is_ranging=is_rng,
            range_signal=range_signal, stat_analyzer=sym.stat,
            now=now, atr15=atr15,
        )

        # [풀트레일] 부분익절 스킵
        if (pos.strategy == "QPULSE_BB" and exit_reason
                and "익절" in exit_reason and exit_ratio < 0.99):
            exit_reason = None

        if exit_reason:
            _execute_exit(sym, pos, trader, gs, tg, last_price, exit_reason, now)

    # ── 거래소 포지션 동기화 (손절이 거래소에서 먼저 터진 경우 감지) ──
    if sym.positions:
        exch_pos = trader.get_position(sym.symbol)
        if exch_pos is None:
            # 거래소엔 포지션 없음 → 손절 자동 체결된 것
            for pos in sym.positions[:]:
                mark = trader.get_mark_price(sym.symbol) or last_price
                pnl  = pos.partial_exit(pos.remaining_qty, mark, 0)
                sym.perf.balance += pnl
                sym.perf.trades  += 1
                if pnl < 0:
                    sym.perf.consecutive_losses += 1; sym.perf.stoplosses += 1
                else:
                    sym.perf.consecutive_losses = 0
                sym.positions.remove(pos)
                sym.last_exit_time = now; sym.last_exit_side = pos.side
                gs.remove(sym.symbol)
                sym.plan_order_id = None
                _fmt(sym.symbol, pos.side, mark, 0, 0, pnl, "거래소손절자동체결", tg)

    # ── 진입 판단 ─────────────────────────────────────────────
    if sym.positions:
        return  # 이미 포지션 있음

    decision = S.decide_entry(
        p_up=p_up, delta_p=delta_p, accel=accel,
        sync_score=sync_score, is_ranging=is_rng,
        range_signal=range_signal, stat_analyzer=sym.stat,
        perf=sym.perf, feedback=sym.feedback, reversion_tracker=sym.reversion,
        qpulse_tracker=sym.qpulse_tracker, qpulse_signal=qpulse_signal,
        qp_armed_tracker=sym.qp_armed_tracker, qp_armed_signal=qp_armed_signal,
        now=now, last_price=last_price, atr15=atr15, avg_atr=avg_atr, tf=tf,
        last_exit_time=sym.last_exit_time, last_exit_side=sym.last_exit_side,
    )

    # QPULSE_BB 신호만 사용 (다른 전략 차단)
    if decision.should_enter and decision.strategy != "QPULSE_BB":
        decision.should_enter = False

    if not decision.should_enter:
        return

    # 전체 포지션 한도 체크
    if not gs.can_enter(sym.symbol):
        print(f"[{S.fmt_time()}] [{sym.symbol}] 스킵 — 전역 포지션 {gs.count()}/{MAX_GLOBAL_POS} 도달")
        return

    # 방향 반전 (백테스트 추세추종 방향)
    decision = _flip(decision, last_price)

    # 포지션 사이즈 계산
    try:
        balance = trader.get_usdt_balance()
    except Exception as e:
        print(f"[{S.fmt_time()}] [{sym.symbol}] 잔고 조회 실패: {e}")
        return

    notional = balance * POS_RATIO * TARGET_LEVERAGE
    qty      = notional / last_price
    dec      = QTY_DEC.get(sym.symbol, 3)
    qty      = math.floor(qty * 10**dec) / 10**dec   # 내림 (초과 주문 방지)

    if qty <= 0:
        print(f"[{S.fmt_time()}] [{sym.symbol}] 수량 0 — 잔고 부족")
        return

    # 진입 주문 (지정가 5틱)
    order_id = trader.place_entry(sym.symbol, decision.side, qty, last_price)
    if order_id is None:
        return

    # 손절 주문 등록
    plan_id = trader.place_stop_loss(
        sym.symbol, decision.side, qty, decision.stop_price
    )

    # 봇 상태 업데이트
    sym.signal_counter += 1
    fees_est = S.fee(notional / TARGET_LEVERAGE)  # 수수료 추정
    sym.perf.balance -= fees_est
    pos = S.SplitPosition(side=decision.side, strategy="QPULSE_BB",
                          signal_id=sym.signal_counter)
    pos.add_entry(last_price, qty, now, fees_est)
    pos.initial_stop = decision.stop_price
    pos.qpulse_bb_middle = 0.0
    pos._tightened = False
    pos.trailing.init(last_price, decision.side, decision.stop_price, atr15)
    sym.positions.append(pos)
    sym.plan_order_id = plan_id
    sym.live_qty = qty
    gs.add(sym.symbol)

    _fmt(sym.symbol, decision.side, last_price, decision.stop_price, qty, None, None, tg)


# =============================================================================
# 청산 실행 헬퍼
# =============================================================================
def _execute_exit(sym: BotSymbolState, pos: S.SplitPosition,
                  trader: BitgetTrader, gs: GlobalState,
                  tg, exit_price: float, reason: str, now: datetime):
    # 거래소 손절 주문 취소 후 시장가 청산
    if sym.plan_order_id:
        trader.cancel_stop_loss(sym.symbol, sym.plan_order_id)
        sym.plan_order_id = None

    trader.close_position(sym.symbol, pos.side, sym.live_qty or pos.remaining_qty)

    # 봇 상태 업데이트
    fees  = S.fee(pos.remaining_qty * exit_price)
    sym.perf.balance -= fees
    pnl   = pos.partial_exit(pos.remaining_qty, exit_price, fees)
    sym.perf.balance += pnl
    sym.perf.trades  += 1
    if pnl < 0:
        sym.perf.consecutive_losses += 1; sym.perf.stoplosses += 1
    else:
        sym.perf.consecutive_losses = 0
    sym.positions.remove(pos)
    sym.last_exit_time = now
    sym.last_exit_side = pos.side
    sym.live_qty = 0.0
    gs.remove(sym.symbol)

    _fmt(sym.symbol, pos.side, exit_price, 0, 0, pnl, reason, tg)


# =============================================================================
# 시작 시 거래소 포지션 동기화
# =============================================================================
def sync_on_startup(states: list, trader: BitgetTrader, gs: GlobalState, tg):
    print(f"[{S.fmt_time()}] 시작 시 Bitget 포지션 동기화...")
    for sym in states:
        exch = trader.get_position(sym.symbol)
        if exch:
            qty  = float(exch.get("total", 0))
            side = "LONG" if exch.get("holdSide") == "long" else "SHORT"
            avg  = float(exch.get("openPriceAvg", 0))
            print(f"  [{sym.symbol}] 기존 포지션 감지: {side} {qty} @ {avg}")
            now = datetime.now(timezone.utc)
            pos = S.SplitPosition(side=side, strategy="QPULSE_BB", signal_id=0)
            pos.add_entry(avg, qty, now, 0.0)
            pos.initial_stop = avg * (0.97 if side == "LONG" else 1.03)  # 임시 3% 손절
            pos.qpulse_bb_middle = 0.0
            pos._tightened = False
            sym.positions.append(pos)
            sym.live_qty = qty
            gs.add(sym.symbol)
            msg = f"[{sym.symbol}] 기존 포지션 인수: {side} {qty} @ {avg}"
            tg.send(msg)


# =============================================================================
# 메인
# =============================================================================
def main():
    mode_str = "🟡 DRY RUN 모드 (실주문 없음)" if DRY_RUN else "🔴 실매매 모드"
    print("=" * 65)
    print(f"  D3x50 실매매 봇  |  {mode_str}")
    print(f"  심볼: {' / '.join(SYMBOLS)}")
    print(f"  레버리지: {TARGET_LEVERAGE}x  |  포지션: 계좌 {POS_RATIO*100:.0f}%/건  |  최대 {MAX_GLOBAL_POS}개")
    print(f"  전략: QPULSE_BB  D3 필터  (추세추종 방향)")
    print(f"  갱신주기: {REFRESH_SEC}초")
    print("=" * 65)

    tg     = S.TelegramBot(S.TELEGRAM_TOKEN, S.TELEGRAM_CHAT_ID)
    trader = BitgetTrader()
    gs     = GlobalState()
    states = [BotSymbolState(symbol=s) for s in SYMBOLS]

    # 심볼별 레버리지 초기 설정
    for sym in states:
        try:
            trader.init_symbol(sym.symbol)
            print(f"  [{sym.symbol}] 레버리지 {TARGET_LEVERAGE}x 설정 완료")
        except Exception as e:
            print(f"  [{sym.symbol}] 레버리지 설정 오류: {e}")

    # 기존 포지션 인수
    sync_on_startup(states, trader, gs, tg)

    tg.send(
        f"D3x50 봇 시작  {mode_str}\n"
        f"심볼: {', '.join(SYMBOLS)}\n"
        f"레버리지: {TARGET_LEVERAGE}x  최대 {MAX_GLOBAL_POS}개 동시 포지션\n"
        f"포지션 현황: {gs.count()}/{MAX_GLOBAL_POS}"
    )

    retry_count = 0

    while True:
        try:
            now = datetime.now(timezone.utc)

            for sym in states:
                process_symbol(sym, trader, gs, tg, now)
                time.sleep(0.3)   # 심볼 간 API 호출 간격

            # 상태 출력 (1분마다)
            if int(now.timestamp()) % 60 < REFRESH_SEC:
                pos_info = " | ".join(
                    f"{s.symbol}: {'OPEN '+s.positions[0].side if s.positions else 'none'}"
                    for s in states
                )
                print(f"[{S.fmt_time()}] 포지션: {gs.count()}/{MAX_GLOBAL_POS}  |  {pos_info}")

            retry_count = 0
            time.sleep(REFRESH_SEC)

        except KeyboardInterrupt:
            print(f"\n[{S.fmt_time()}] 봇 종료 요청")
            tg.send("D3x50 봇 수동 종료")
            break

        except Exception as e:
            retry_count += 1
            print(f"[{S.fmt_time()}] 오류 ({retry_count}/{MAX_RETRIES}): {type(e).__name__}: {e}")
            print(traceback.format_exc())
            if retry_count >= MAX_RETRIES:
                tg.send(f"D3x50 봇 {MAX_RETRIES}회 연속 오류. 종료.\n{e}")
                break
            time.sleep(REFRESH_SEC)


if __name__ == "__main__":
    main()
