# -*- coding: utf-8 -*-
"""
Bybit BTC 고급 매매봇 v7 — 회귀 전략 통합

[v7 핵심 변경: 재입학 회귀 전략]
🔴 "벗어날 때가 아니라, 돌아올 때 진입"
   - 이탈(deviation) 감지: p_up이 극단으로 이동 → 진입하지 않고 플래그만 기록
   - 회귀(reversion) 확인: delta_p 부호 반전 + p_up이 0.5 방향으로 복귀 시작 → 진입
   - 가속도(accel)까지 회귀 방향이면 확신도 상승
🔴 승률 70% 이상 유지
   - 회귀 전략 승률 별도 추적
   - 70% 미만 시 진입 기준 자동 강화 (더 강한 회귀 확인 요구)
   - 60% 미만 시 회귀 전략 일시 중단
🔴 손절 = 이탈 극값 기반 (짧고 명확)
   - 이탈 시 기록한 극단 가격 바로 바깥에 손절
   - 짧은 손절 + 높은 승률 = 손절을 수익으로 녹이는 구조
🔴 익절 = 보수적 (평균 회귀 목표)
   - 1차 목표: 이탈 거리의 50% 회복
   - 2차 목표: 평균(BB중심/EMA20) 도달
   - 욕심내지 않고 빈번한 승리 추구

[v6에서 유지]
✅ 가우시안 커널 회귀 (진입 에지 계산)
✅ 물타기 방지 (손실 -0.05% 이상 시 추가진입 금지)
✅ 조기손절 완화 (15사이클, MAE×85%, 최소 -0.25%)
✅ 시간정지 완화 (최소 10분, 중앙×3/×5)
✅ 트레일링 완화 (0.35%까지 고정, ATR×0.8 최소)
✅ 재진입 쿨다운 (2분/3분)
"""

import time
import math
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from collections import deque
from typing import Optional, List, Tuple

import threading
import queue as _queue

import requests
import numpy as np
import pandas as pd
from scipy import stats as sp_stats


# =========================
# 사용자 설정
# =========================
# 텔레그램 설정
TELEGRAM_TOKEN   = "8569294541:AAEcC93ouZv2fqmHLj_bXJ1P4ZobzfCnCyk"
TELEGRAM_CHAT_ID = "7875605749"

SYMBOL = "BTCUSDT"
CATEGORY = "linear"
BYBIT_REST = "https://api.bybit.com"

REFRESH_SEC = 20

TIMEFRAMES = {"1m": "1", "5m": "5", "15m": "15", "1h": "60", "4h": "240"}
CANDLES_LIMIT = 420

CACHE_SECONDS = {"1m": 60, "5m": 90, "15m": 180, "1h": 600, "4h": 1800}
WEIGHTS = {"4h": 0.40, "1h": 0.30, "15m": 0.13, "5m": 0.12, "1m": 0.05}

# 횡보
RANGE_ADX_THRESHOLD = 20
RANGE_BB_TOUCH = 0.15
RANGE_RSI_OVERSOLD = 40
RANGE_RSI_OVERBOUGHT = 60
RANGE_RSI_EXIT = 50

# 손절 기본값
STOP_ATR_MULT = 3.0

# 가상매매
START_BALANCE = 1000.0
BASE_POSITION_RATIO = 0.63
LEVERAGE = 1.0

# 비용
TAKER_FEE_RATE = 0.0005   # Binance 선물 표준 테이커 수수료 0.05%
SLIPPAGE_BPS = 0.5
TOTAL_COST_PCT = (TAKER_FEE_RATE * 2 + SLIPPAGE_BPS / 10000.0 * 2) * 100

# 리스크 관리
MAX_DRAWDOWN_PCT = 15.0
MAX_CONSECUTIVE_LOSSES = 4
COOLDOWN_MINUTES = 30

# 통계 시스템
PRIOR_STRENGTH = 10
TIME_DECAY_LAMBDA = 0.002
CONFIDENCE_LEVEL = 0.95

# 분할 진입/청산
SPLIT_ENTRY_RATIOS = [0.4, 0.3, 0.3]
SPLIT_ENTRY_CONFIRM_PCT = 0.1
SPLIT_EXIT_RATIOS = [0.25, 0.25, 0.50]

# 트레일링 스탑 기본 구간
TRAIL_ZONES_DEFAULT = [
    (0.0, 0.3, None),
    (0.3, 0.8, 0.0),
    (0.8, 1.5, 0.50),
    (1.5, 999.0, 0.70),
]

# 모멘텀 적응형 트레일링
TRAIL_ACCEL_WIDE = 1.6
TRAIL_ACCEL_NORMAL = 1.0
TRAIL_ACCEL_TIGHT = 0.5

# 시간 기반 청산
TIME_STOP_STALE_MULT = 5.0
TIME_STOP_BREAKEVEN_MULT = 3.0
TIME_STOP_MIN_HOLD_SEC = 600

# 조기 손절
EARLY_CUT_CYCLES = 15
EARLY_CUT_MAE_MULT = 0.85
EARLY_CUT_MIN_LOSS_PCT = 0.25

# MFE 기반 다단계 익절
TP_STAGE1_PERCENTILE = 40
TP_STAGE2_PERCENTILE = 65
TP_STAGE3_USE_TRAIL = True

# 추세 연장 조건
TREND_EXTEND_SYNC_MIN = 3
TREND_EXTEND_ACCEL_MIN = 0.005

# 추세 진입 필터 (v8 개선)
TREND_MIN_SCORE = 2.0       # 모멘텀 폴백 최소 점수 (높을수록 선별적, 기존 1.0)
TREND_MIN_ADX   = 22        # 추세 진입 최소 ADX — 이 미만이면 TREND 거부

# 전략 활성화 스위치
STRATEGY_ALLOW_TREND = False   # TREND 진입 허용 여부 (False = 비활성화)
STRATEGY_ALLOW_RANGE = False   # RANGE 진입 허용 여부 (False = 비활성화)

# 재진입 쿨다운
REENTRY_COOLDOWN_SEC = 120
REENTRY_SAME_DIR_COOLDOWN_SEC = 180

# 동시 추적 최대 신호 수
MAX_CONCURRENT_SIGNALS = 3

# 성과 피드백
FEEDBACK_WINDOW = 10

# ============================================================
# 🆕 v7: 회귀 전략 설정
# ============================================================
# 이탈 감지 임계값
REVERSION_DEVIATION_THRESHOLD = 0.051  # p_up이 0.5에서 ±0.051 이상 벗어나면 이탈
REVERSION_EXTREME_THRESHOLD = 0.15     # p_up이 0.5에서 ±0.15 이상이면 강한 이탈

# 회귀 확인 조건
REVERSION_CONFIRM_DELTA_P = 0.015     # delta_p가 회귀 방향으로 최소 이만큼
REVERSION_CONFIRM_CYCLES = 2          # 연속 N사이클 회귀 방향 delta_p
REVERSION_STRONG_CONFIRM_CYCLES = 2   # 강한 확인: 2사이클 연속 (완화)

# 회귀 가속 확인
REVERSION_ACCEL_BONUS = 0.005         # 가속도가 회귀 방향이면 보너스

# 승률 관리
REVERSION_TARGET_WINRATE = 70.0       # 목표 승률 70%
REVERSION_WARN_WINRATE = 65.0         # 경고: 기준 강화
REVERSION_PAUSE_WINRATE = 55.0        # 일시 중단
REVERSION_MIN_TRADES = 10             # 승률 계산 최소 거래 수

# 회귀 익절 (보수적)
REVERSION_TP1_RATIO = 0.55            # 1차 익절: 이탈 거리의 55% 회복
REVERSION_TP2_RATIO = 0.85            # 2차 익절: 이탈 거리의 85% 회복

# 회귀 손절 (이탈 극값 기반)
REVERSION_SL_BUFFER_PCT = 0.086       # 이탈 극값에서 버퍼 % 바깥에 손절

SHOW_DEBUG = False
SIGNAL_ONLY = True   # True: 시그널 발생 시에만 출력 (조용한 감시 모드)
MAX_RETRIES = 3
UA = {"User-Agent": "trend-bot-v2/1.0"}

# ============================================================
# [lab1 융합] Q-Pulse + BB 역추세 전략 설정
# ============================================================
# Q-Pulse 지표 파라미터
QPULSE_LPERIOD = 15          # WMA 기간 (변동성 평균)
QPULSE_PER = 0.3             # 방향성 비율 임계값

# EMA 교차 파라미터 (더 민감한 12/26)
QPULSE_EMA_SHORT = 12
QPULSE_EMA_LONG = 26

# Setup 로직
QPULSE_MAX_LOOKBACK = 15     # EMA 교차 이전 Q-Pulse 탐색 범위 (캔들 수, 완화)

# 손절/익절
QPULSE_SL_ATR_MULT = 2.5    # 손절: ATR x 배수 (최적화: 1.9→2.8→2.5)
# 1차 익절은 BB 중심선 도달 시 (SPLIT_EXIT_RATIOS[0] 비율)

# 승률 관리
QPULSE_MIN_TRADES = 5        # 승률 계산 최소 거래 수

# [v8] QP-BB 진입 필터
QPULSE_BB_BAND_EDGE_RATIO = 0.28  # BB 밴드 외곽 28% 영역에서만 진입
QPULSE_BB_VOL_MULT        = 1.32  # 진입 시 거래량이 20봉 평균의 1.32배 이상이어야 함
QPULSE_BB_RSI_LONG_MAX    = 36    # QPULSE_BB LONG 진입 최대 RSI (최적화: 42→38→36)
QPULSE_BB_RSI_SHORT_MIN   = 65    # QPULSE_BB SHORT 진입 최소 RSI (최적화: 58→65)

# ============================================================
# [v8] Q-Pulse Armed 회귀 전략 설정
# ============================================================
# 멀티TF Q-Pulse 가중치 (4h 제외: 너무 느림)
QPULSE_MTF_WEIGHTS = {"1h": 0.40, "15m": 0.30, "5m": 0.20, "1m": 0.10}

# ARMED 진입 임계값
QPULSE_MTF_ARMED_THRESHOLD  = 0.35   # 가중합 절대값 이 이상이면 ARMED
QPULSE_MTF_MIN_EXTEND_PCT   = 0.10   # ARMED 후 가격이 이 % 이상 연장되어야 EXTENDED
QPULSE_MTF_ARMED_EXPIRE_SEC = 300    # 5분 내 EXTENDED 미진입 시 만료

# 회귀 확인
QPULSE_MTF_CONFIRM_DELTA_P  = 0.013  # 회귀 방향 delta_p 최소값
QPULSE_MTF_CONFIRM_CYCLES   = 1      # 연속 확인 사이클

# 손절/익절
QPULSE_MTF_SL_BUFFER_PCT    = 0.094  # 극값에서 손절 버퍼 %
QPULSE_MTF_TP1_RATIO        = 0.48   # 1차 익절: 이탈거리 48% 회복
QPULSE_MTF_TP2_RATIO        = 0.80   # 2차 익절: 이탈거리 80% 회복

# 기본 신뢰도 (일반 회귀 0.50 대비 높음)
QPULSE_MTF_BASE_CONFIDENCE  = 0.55

# 승률 관리
QPULSE_MTF_MIN_TRADES       = 8
QPULSE_MTF_PAUSE_WINRATE    = 50.0


# =========================
# REST 유틸
# =========================
def bybit_get(path: str, params: dict) -> dict:
    url = f"{BYBIT_REST}{path}"
    r = requests.get(url, params=params, headers=UA, timeout=12)
    r.raise_for_status()
    js = r.json()
    if js.get("retCode") != 0:
        raise RuntimeError(f"Bybit error: {js.get('retCode')} {js.get('retMsg')}")
    return js


def fetch_klines(symbol: str, interval: str, limit: int, category: str = "linear") -> pd.DataFrame:
    js = bybit_get("/v5/market/kline", {"category": category, "symbol": symbol, "interval": interval, "limit": limit})
    rows = js["result"]["list"]
    df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "turnover"])
    for c in ["open", "high", "low", "close", "volume", "turnover"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["ts"] = pd.to_numeric(df["ts"], errors="coerce").astype("int64")
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df.sort_values("dt").reset_index(drop=True)


def fetch_last_price(symbol: str, category: str = "linear") -> float:
    js = bybit_get("/v5/market/tickers", {"category": category, "symbol": symbol})
    lst = js["result"]["list"]
    if not lst:
        raise RuntimeError("ticker empty")
    return float(lst[0]["lastPrice"])


# =========================
# 지표
# =========================
def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()

def sma(s: pd.Series, period: int) -> pd.Series:
    return s.rolling(window=period).mean()

def rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.ewm(alpha=1/period, adjust=False).mean()
    roll_down = down.ewm(alpha=1/period, adjust=False).mean().replace(0, np.nan)
    rs = roll_up / roll_down
    return 100 - (100 / (1 + rs))

def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    return pd.concat([
        (df["high"] - df["low"]).abs(),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs(),
    ], axis=1).max(axis=1)

def atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    return true_range(df).ewm(alpha=1/period, adjust=False).mean()

def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    high, low = df["high"], df["low"]
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_s = true_range(df).ewm(alpha=1/period, adjust=False).mean().replace(0, np.nan)
    plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1/period, adjust=False).mean() / atr_s
    minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1/period, adjust=False).mean() / atr_s
    dx = 100 * (plus_di - minus_di).abs() / ((plus_di + minus_di).replace(0, np.nan))
    return dx.ewm(alpha=1/period, adjust=False).mean()

def macd_hist(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.Series:
    macd_line = ema(close, fast) - ema(close, slow)
    signal_line = ema(macd_line, signal)
    return macd_line - signal_line

def bollinger_bands(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> tuple:
    middle = sma(close, period)
    std = close.rolling(window=period).std()
    upper = middle + (std * std_dev)
    lower = middle - (std * std_dev)
    return upper, middle, lower


# [lab1 융합] Q-Pulse 지표 함수
def wma(s: pd.Series, period: int) -> pd.Series:
    """가중이동평균 (Weighted Moving Average)"""
    weights = np.arange(1, period + 1, dtype=float)
    return s.rolling(window=period, min_periods=period).apply(
        lambda x: np.dot(x, weights) / weights.sum(), raw=True
    )

def q_pulse_series(df: pd.DataFrame,
                   lperiod: int = QPULSE_LPERIOD,
                   per: float = QPULSE_PER) -> pd.Series:
    """
    Q-Pulse: 변동성이 평균 이상이면서 캔들 방향성이 강한 구간을 감지.
    반환값: 1(강한 상승), -1(강한 하락), 0(중립)
    """
    percent_hl = ((df["high"] - df["low"]) / df["close"]) * 100

    mask_bear = df["open"] > df["close"]
    mask_bull = df["open"] < df["close"]

    percent_red = pd.Series(0.0, index=df.index)
    percent_green = pd.Series(0.0, index=df.index)
    percent_red[mask_bear] = ((df["open"] - df["close"]) / df["close"]) * 100
    percent_green[mask_bull] = ((df["close"] - df["open"]) / df["open"]) * 100

    avg_hl = wma(percent_hl, lperiod)

    result = pd.Series(0, index=df.index, dtype=int)
    vol_cond = percent_hl > avg_hl
    bull_cond = vol_cond & ((percent_hl * per) < percent_green)
    bear_cond = vol_cond & ((percent_hl * per) < percent_red)
    result[bull_cond] = 1
    result[bear_cond] = -1
    return result


# =========================
# 추세 점수화
# =========================
def classify_trend(df: pd.DataFrame) -> dict:
    df = df.copy()
    c = df["close"]
    df["ema20"] = ema(c, 20)
    df["ema50"] = ema(c, 50)
    df["ema200"] = ema(c, 200)
    df["rsi14"] = rsi(c, 14)
    df["atr14"] = atr(df, 14)
    df["adx14"] = adx(df, 14)
    df["mh"] = macd_hist(c)
    bb_upper, bb_middle, bb_lower = bollinger_bands(c, 20, 2.0)
    df["bb_upper"], df["bb_middle"], df["bb_lower"] = bb_upper, bb_middle, bb_lower

    last = df.iloc[-1]
    ema_stack_up = df["ema20"].iloc[-1] > df["ema50"].iloc[-1] > df["ema200"].iloc[-1]
    ema_stack_dn = df["ema20"].iloc[-1] < df["ema50"].iloc[-1] < df["ema200"].iloc[-1]

    mh = float(df["mh"].iloc[-1]) if pd.notna(df["mh"].iloc[-1]) else 0.0
    adx_val = float(df["adx14"].iloc[-1]) if pd.notna(df["adx14"].iloc[-1]) else 0.0
    rsi_val = float(df["rsi14"].iloc[-1]) if pd.notna(df["rsi14"].iloc[-1]) else 50.0
    atr_val = float(df["atr14"].iloc[-1]) if pd.notna(df["atr14"].iloc[-1]) else 0.0
    bb_u = float(last["bb_upper"]) if pd.notna(last["bb_upper"]) else 0.0
    bb_m = float(last["bb_middle"]) if pd.notna(last["bb_middle"]) else 0.0
    bb_l = float(last["bb_lower"]) if pd.notna(last["bb_lower"]) else 0.0

    # [lab1 융합] EMA 19/40 + Q-Pulse 계산
    df["ema19"] = ema(c, QPULSE_EMA_SHORT)
    df["ema40"] = ema(c, QPULSE_EMA_LONG)
    qp = q_pulse_series(df)
    ema19_val = float(df["ema19"].iloc[-1]) if pd.notna(df["ema19"].iloc[-1]) else float(last["close"])
    ema40_val = float(df["ema40"].iloc[-1]) if pd.notna(df["ema40"].iloc[-1]) else float(last["close"])
    ema19_prev = float(df["ema19"].iloc[-2]) if len(df) >= 2 and pd.notna(df["ema19"].iloc[-2]) else ema19_val
    ema40_prev = float(df["ema40"].iloc[-2]) if len(df) >= 2 and pd.notna(df["ema40"].iloc[-2]) else ema40_val
    qpulse_val = int(qp.iloc[-1]) if pd.notna(qp.iloc[-1]) else 0

    if ema_stack_up and mh > 0:
        label, direction_score = "UP", 65
    elif ema_stack_dn and mh < 0:
        label, direction_score = "DOWN", 5
    else:
        label, direction_score = "NEUTRAL", 35

    strength_score = 20 if adx_val >= 35 else (12 if adx_val >= 25 else (6 if adx_val >= 15 else 0))

    momentum_score = 0
    if label == "UP":
        momentum_score = 10 if 50 <= rsi_val <= 70 else (4 if rsi_val > 70 else 0)
    elif label == "DOWN":
        momentum_score = 10 if 30 <= rsi_val <= 50 else (4 if rsi_val < 30 else 0)

    score = int(round(np.clip(direction_score + strength_score + momentum_score, 0, 100)))

    vol_val    = float(last["volume"]) if pd.notna(last["volume"]) else 0.0
    vol_ma20   = float(df["volume"].rolling(20).mean().iloc[-1]) if len(df) >= 20 else vol_val

    return {
        "label": label, "score": score, "close": float(last["close"]),
        "adx14": adx_val, "rsi14": rsi_val, "atr14": atr_val,
        "bb_upper": bb_u, "bb_middle": bb_m, "bb_lower": bb_l,
        "volume": vol_val, "vol_ma20": vol_ma20,
        "time": last["dt"].to_pydatetime(),
        # [lab1 융합] Q-Pulse 데이터
        "ema19": ema19_val, "ema40": ema40_val,
        "ema19_prev": ema19_prev, "ema40_prev": ema40_prev,
        "qpulse": qpulse_val,
    }


def combine_timeframes(tf: dict) -> dict:
    trend_score = sum(tf[k]["score"] * w for k, w in WEIGHTS.items())
    trend_score = float(np.clip(trend_score, 0, 100))
    p_up_base = float(np.clip(trend_score / 100.0, 0.0, 1.0))
    is_ranging = tf["15m"]["adx14"] < RANGE_ADX_THRESHOLD
    return {"trend_score": trend_score, "p_up_base": p_up_base, "is_ranging": is_ranging}


def calc_qpulse_multitf_score(tf: dict) -> float:
    """
    1m/5m/15m/1h 각 TF의 Q-Pulse 값을 가중합하여 멀티TF 방향 강도 반환.
    양수 = 강한 상승 펄스, 음수 = 강한 하락 펄스.
    절대값이 QPULSE_MTF_ARMED_THRESHOLD 이상이면 ARMED 조건 충족.
    """
    score = 0.0
    for tf_key, weight in QPULSE_MTF_WEIGHTS.items():
        if tf_key in tf and "qpulse" in tf[tf_key]:
            score += tf[tf_key]["qpulse"] * weight
    return float(score)


# =========================
# 횡보 매매 신호
# =========================
def check_range_signal(tf: dict, rsi_oversold: float, rsi_overbought: float) -> dict:
    data = tf["15m"]
    close, rsi_val = data["close"], data["rsi14"]
    bb_upper, bb_lower = data["bb_upper"], data["bb_lower"]
    bb_width = bb_upper - bb_lower
    if bb_width == 0:
        return {"action": None}
    bb_position = (close - bb_lower) / bb_width

    if bb_position <= RANGE_BB_TOUCH and rsi_val < rsi_oversold:
        return {"action": "LONG", "reason": f"BB하단({bb_position:.2f})+RSI({rsi_val:.1f})"}
    if bb_position >= (1.0 - RANGE_BB_TOUCH) and rsi_val > rsi_overbought:
        return {"action": "SHORT", "reason": f"BB상단({bb_position:.2f})+RSI({rsi_val:.1f})"}
    if 0.45 <= bb_position <= 0.55 and abs(rsi_val - RANGE_RSI_EXIT) < 10:
        return {"action": "EXIT_BOTH", "reason": f"BB중심({bb_position:.2f})+RSI({rsi_val:.1f})"}
    return {"action": None}


# =========================
# 온라인 캘리브레이션
# =========================
def sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    else:
        z = math.exp(x)
        return z / (1.0 + z)

def logit(p: float) -> float:
    p = float(np.clip(p, 1e-6, 1 - 1e-6))
    return math.log(p / (1 - p))


@dataclass
class Calibrator:
    bias: float = 0.0
    scale: float = 1.0
    lr: float = 0.08
    scale_min: float = 0.5
    scale_max: float = 2.0
    last_15m_start: Optional[datetime] = None
    pending_pred: Optional[float] = None
    pending_p_base: Optional[float] = None
    pending_close: Optional[float] = None
    n: int = 0
    hit: int = 0

    def calibrate(self, p_base: float) -> float:
        x = logit(p_base)
        p = sigmoid(self.scale * x + self.bias)
        return float(np.clip(p, 0.0, 1.0))

    def update_on_15m_close(self, current_15m_start: datetime, current_close: float, p_base_now: float):
        if self.last_15m_start is None:
            self.last_15m_start = current_15m_start
            self.pending_pred = self.calibrate(p_base_now)
            self.pending_p_base = p_base_now
            self.pending_close = current_close
            return
        if current_15m_start != self.last_15m_start:
            if self.pending_pred is not None and self.pending_close is not None and self.pending_p_base is not None:
                y = 1.0 if (current_close > self.pending_close) else 0.0
                p = float(np.clip(self.pending_pred, 1e-6, 1 - 1e-6))
                x = logit(self.pending_p_base)
                grad = (y - p)
                self.bias += self.lr * grad
                self.scale += self.lr * grad * x
                self.scale = float(np.clip(self.scale, self.scale_min, self.scale_max))
                self.n += 1
                self.hit += int((p >= 0.5) == (y == 1.0))
            self.last_15m_start = current_15m_start
            self.pending_pred = self.calibrate(p_base_now)
            self.pending_p_base = p_base_now
            self.pending_close = current_close


# ============================================================
# 통계 수집 시스템
# ============================================================
@dataclass
class StatRecord:
    timestamp: datetime
    p_up: float
    delta_p: float
    delta_p_speed: float
    delta_p_accel: float
    price: float
    sync_score: int
    price_1m: Optional[float] = None
    price_5m: Optional[float] = None
    price_15m: Optional[float] = None
    max_favorable: Optional[float] = None
    max_adverse: Optional[float] = None
    filled_1m: bool = False
    filled_5m: bool = False
    filled_15m: bool = False
    mfe_reached_at: Optional[float] = None
    mfe_peak_price: Optional[float] = None
    trajectory_mfe: list = field(default_factory=list)
    trajectory_mae: list = field(default_factory=list)


def _p_up_bin(p_up: float) -> int:
    if p_up < 0.35:
        return 0
    elif p_up < 0.45:
        return 1
    elif p_up < 0.55:
        return 2
    elif p_up < 0.65:
        return 3
    else:
        return 4


def _delta_p_bin(dp: float) -> int:
    if dp <= -0.08:
        return 0
    elif dp <= -0.03:
        return 1
    elif dp < 0.03:
        return 2
    elif dp < 0.08:
        return 3
    else:
        return 4


def _accel_bin(accel: float) -> int:
    if accel < -0.01:
        return 0
    elif accel <= 0.01:
        return 1
    else:
        return 2


@dataclass
class StatAnalyzer:
    records: deque = field(default_factory=lambda: deque(maxlen=5000))
    total_records: int = 0
    decay_lambda: float = TIME_DECAY_LAMBDA
    base_decay_lambda: float = TIME_DECAY_LAMBDA
    recent_predictions: deque = field(default_factory=lambda: deque(maxlen=30))

    def add_record(self, rec: StatRecord):
        self.records.append(rec)
        self.total_records += 1

    def fill_future_prices(self, current_price: float, now: datetime):
        for rec in self.records:
            if rec.filled_15m:
                continue
            elapsed = (now - rec.timestamp).total_seconds()
            pct_change = (current_price - rec.price) / rec.price * 100
            if rec.p_up >= 0.5:
                fav = pct_change
                adv = -pct_change
            else:
                fav = -pct_change
                adv = pct_change

            old_mfe = rec.max_favorable or 0.0
            new_mfe = max(old_mfe, fav)
            if new_mfe > old_mfe:
                rec.max_favorable = new_mfe
                rec.mfe_reached_at = elapsed
                rec.mfe_peak_price = current_price
            rec.max_adverse = max(rec.max_adverse or 0.0, adv)

            if len(rec.trajectory_mfe) == 0 or (elapsed - (rec.trajectory_mfe[-1][0] if rec.trajectory_mfe else 0)) >= 20:
                rec.trajectory_mfe.append((elapsed, rec.max_favorable or 0.0))
                rec.trajectory_mae.append((elapsed, rec.max_adverse or 0.0))

            if not rec.filled_1m and elapsed >= 60:
                rec.price_1m = current_price
                rec.filled_1m = True
            if not rec.filled_5m and elapsed >= 300:
                rec.price_5m = current_price
                rec.filled_5m = True
            if not rec.filled_15m and elapsed >= 900:
                rec.price_15m = current_price
                rec.filled_15m = True

    def update_mfe_mae_realtime(self, current_price: float):
        for rec in self.records:
            if rec.filled_15m:
                continue
            pct = (current_price - rec.price) / rec.price * 100
            if rec.p_up >= 0.5:
                fav, adv = pct, -pct
            else:
                fav, adv = -pct, pct
            rec.max_favorable = max(rec.max_favorable or 0, fav)
            rec.max_adverse = max(rec.max_adverse or 0, adv)

    def _get_time_weights(self, now: datetime) -> np.ndarray:
        weights = []
        for rec in self.records:
            elapsed_min = (now - rec.timestamp).total_seconds() / 60.0
            w = math.exp(-self.decay_lambda * elapsed_min)
            weights.append(w)
        return np.array(weights)

    def _effective_sample_size(self, weights: np.ndarray) -> float:
        if len(weights) == 0 or np.sum(weights) == 0:
            return 0.0
        return (np.sum(weights) ** 2) / np.sum(weights ** 2)

    def query_bin(self, p_up: float, delta_p: float, accel: float,
                  now: datetime, lookback_minutes: int = 720) -> dict:
        pb = _p_up_bin(p_up)
        db = _delta_p_bin(delta_p)
        ab = _accel_bin(accel)
        cutoff = now - timedelta(minutes=lookback_minutes)

        returns_15m = []
        weights = []
        mfe_list = []
        mae_list = []

        for rec in self.records:
            if rec.timestamp < cutoff:
                continue
            if not rec.filled_15m:
                continue
            if _p_up_bin(rec.p_up) != pb:
                continue
            if _delta_p_bin(rec.delta_p) != db:
                continue
            if _accel_bin(rec.delta_p_accel) != ab:
                continue

            ret = (rec.price_15m - rec.price) / rec.price * 100.0
            if rec.p_up < 0.5:
                ret = -ret

            elapsed_min = (now - rec.timestamp).total_seconds() / 60.0
            w = math.exp(-self.decay_lambda * elapsed_min)

            returns_15m.append(ret)
            weights.append(w)
            if rec.max_favorable is not None:
                mfe_list.append(rec.max_favorable)
            if rec.max_adverse is not None:
                mae_list.append(rec.max_adverse)

        weights = np.array(weights)
        returns_15m = np.array(returns_15m)
        n_raw = len(returns_15m)
        ess = self._effective_sample_size(weights) if n_raw > 0 else 0.0

        prior_mean = 0.0
        prior_strength = PRIOR_STRENGTH

        if n_raw > 0:
            w_sum = np.sum(weights)
            weighted_mean = np.sum(returns_15m * weights) / w_sum if w_sum > 0 else 0.0
            weighted_var = np.sum(weights * (returns_15m - weighted_mean) ** 2) / w_sum if w_sum > 0 else 1.0
            weighted_std = math.sqrt(max(weighted_var, 1e-8))
            posterior_mean = (prior_strength * prior_mean + ess * weighted_mean) / (prior_strength + ess)
            posterior_std = weighted_std / math.sqrt(max(prior_strength + ess, 1))
        else:
            posterior_mean = prior_mean
            posterior_std = 1.0
            weighted_std = 1.0

        if ess > 2:
            t_crit = sp_stats.t.ppf((1 + CONFIDENCE_LEVEL) / 2, df=max(ess - 1, 1))
            ci_lower = posterior_mean - t_crit * posterior_std
            ci_upper = posterior_mean + t_crit * posterior_std
        else:
            ci_lower = -999.0
            ci_upper = 999.0

        if n_raw > 0:
            wins = np.sum(weights[returns_15m > 0])
            total_w = np.sum(weights)
            winrate = (wins / total_w * 100) if total_w > 0 else 50.0
        else:
            winrate = 50.0

        mfe_median = float(np.median(mfe_list)) if mfe_list else 0.0
        mfe_p70 = float(np.percentile(mfe_list, 70)) if len(mfe_list) >= 5 else 0.0
        mae_p80 = float(np.percentile(mae_list, 80)) if len(mae_list) >= 5 else 0.0

        return {
            "n_raw": n_raw, "ess": ess,
            "expected_return": posterior_mean, "std": weighted_std,
            "ci_lower": ci_lower, "ci_upper": ci_upper,
            "winrate": winrate,
            "mfe_median": mfe_median, "mfe_p70": mfe_p70, "mae_p80": mae_p80,
            "bin_label": f"pup={pb} dp={db} acc={ab}",
        }

    def get_confidence_tier(self, bin_stats: dict) -> str:
        cost = TOTAL_COST_PCT
        if bin_stats["ci_lower"] > cost:
            return "high"
        elif bin_stats["expected_return"] > cost and bin_stats["ci_lower"] > -cost:
            return "medium"
        else:
            return "low"

    def check_overfit(self):
        if len(self.recent_predictions) < 20:
            return
        recent = list(self.recent_predictions)[-20:]
        accuracy = sum(1 for correct in recent if correct) / len(recent)
        if accuracy < 0.40:
            self.decay_lambda = self.base_decay_lambda * 2.0
        elif accuracy > 0.55:
            self.decay_lambda = self.base_decay_lambda

    def add_prediction_result(self, correct: bool):
        self.recent_predictions.append(correct)
        self.check_overfit()

    def scan_opportunity(self, p_up: float, delta_p: float, accel: float,
                         sync_score: int, now: datetime,
                         lookback_minutes: int = 720) -> dict:
        cutoff = now - timedelta(minutes=lookback_minutes)
        completed = []
        for rec in self.records:
            if rec.timestamp < cutoff:
                continue
            if not rec.filled_15m:
                continue
            completed.append(rec)

        n_completed = len(completed)

        if n_completed < 10:
            return {
                "best_side": None, "long_edge": 0.0, "short_edge": 0.0,
                "confidence": 0.0, "ess": 0.0, "n_used": 0,
                "long_wr": 50.0, "short_wr": 50.0,
                "long_mfe": 0.0, "short_mfe": 0.0,
                "long_mae": 0.0, "short_mae": 0.0,
            }

        bw_scale = max(0.5, min(2.0, 50.0 / max(n_completed, 1)))
        bw_pup = 0.08 * bw_scale
        bw_dp = 0.04 * bw_scale
        bw_acc = 0.02 * bw_scale

        long_returns = []
        short_returns = []
        kernel_weights = []
        mfe_vals = []
        mae_vals = []

        for rec in completed:
            elapsed_min = (now - rec.timestamp).total_seconds() / 60.0
            time_w = math.exp(-self.decay_lambda * elapsed_min)

            d_pup = (rec.p_up - p_up) / bw_pup
            d_dp = (rec.delta_p - delta_p) / bw_dp
            d_acc = (rec.delta_p_accel - accel) / bw_acc
            kernel_w = math.exp(-0.5 * (d_pup**2 + d_dp**2 + d_acc**2))

            sync_bonus = 1.0
            if rec.sync_score * sync_score > 0:
                sync_bonus = 1.2
            elif rec.sync_score * sync_score < 0:
                sync_bonus = 0.8

            total_w = time_w * kernel_w * sync_bonus
            if total_w < 0.001:
                continue

            raw_ret = (rec.price_15m - rec.price) / rec.price * 100.0
            long_returns.append(raw_ret)
            short_returns.append(-raw_ret)
            kernel_weights.append(total_w)
            mfe_vals.append(rec.max_favorable or 0.0)
            mae_vals.append(rec.max_adverse or 0.0)

        if len(kernel_weights) < 3:
            return {
                "best_side": None, "long_edge": 0.0, "short_edge": 0.0,
                "confidence": 0.0, "ess": 0.0, "n_used": 0,
                "long_wr": 50.0, "short_wr": 50.0,
                "long_mfe": 0.0, "short_mfe": 0.0,
                "long_mae": 0.0, "short_mae": 0.0,
            }

        w = np.array(kernel_weights)
        long_r = np.array(long_returns)
        short_r = np.array(short_returns)
        mfe_arr = np.array(mfe_vals)
        mae_arr = np.array(mae_vals)

        w_sum = np.sum(w)
        ess = (w_sum ** 2) / np.sum(w ** 2) if w_sum > 0 else 0.0

        prior_mean = 0.0
        prior_strength = PRIOR_STRENGTH

        long_wm = np.sum(long_r * w) / w_sum if w_sum > 0 else 0.0
        long_edge = (prior_strength * prior_mean + ess * long_wm) / (prior_strength + ess)

        short_wm = np.sum(short_r * w) / w_sum if w_sum > 0 else 0.0
        short_edge = (prior_strength * prior_mean + ess * short_wm) / (prior_strength + ess)

        long_wins = np.sum(w[long_r > 0])
        long_wr = (long_wins / w_sum * 100) if w_sum > 0 else 50.0
        short_wins = np.sum(w[short_r > 0])
        short_wr = (short_wins / w_sum * 100) if w_sum > 0 else 50.0

        sort_idx = np.argsort(mfe_arr)
        cum_w = np.cumsum(w[sort_idx])
        if cum_w[-1] > 0:
            med_idx = np.searchsorted(cum_w, cum_w[-1] * 0.5)
            med_idx = min(med_idx, len(mfe_arr) - 1)
            w_mfe_med = mfe_arr[sort_idx[med_idx]]
        else:
            w_mfe_med = 0.0

        sort_idx_mae = np.argsort(mae_arr)
        cum_w_mae = np.cumsum(w[sort_idx_mae])
        if cum_w_mae[-1] > 0:
            p80_idx = np.searchsorted(cum_w_mae, cum_w_mae[-1] * 0.8)
            p80_idx = min(p80_idx, len(mae_arr) - 1)
            w_mae_p80 = mae_arr[sort_idx_mae[p80_idx]]
        else:
            w_mae_p80 = 0.0

        if ess > 2:
            long_var = np.sum(w * (long_r - long_wm)**2) / w_sum
            short_var = np.sum(w * (short_r - short_wm)**2) / w_sum
            best_var = long_var if abs(long_edge) >= abs(short_edge) else short_var
            se = math.sqrt(max(best_var, 1e-8)) / math.sqrt(max(ess, 1))
            best_edge = max(abs(long_edge), abs(short_edge))
            t_stat = best_edge / max(se, 1e-6)
            confidence = min(1.0, t_stat / 3.0)
        else:
            confidence = 0.0

        cost = TOTAL_COST_PCT
        best_side = None

        if long_edge > cost and long_edge > short_edge:
            best_side = "LONG"
        elif short_edge > cost and short_edge > long_edge:
            best_side = "SHORT"
        elif long_edge > 0 and long_edge > short_edge and ess >= 20:
            best_side = "LONG"
        elif short_edge > 0 and short_edge > long_edge and ess >= 20:
            best_side = "SHORT"

        return {
            "best_side": best_side,
            "long_edge": long_edge, "short_edge": short_edge,
            "confidence": confidence, "ess": ess, "n_used": len(kernel_weights),
            "long_wr": long_wr, "short_wr": short_wr,
            "long_mfe": w_mfe_med, "short_mfe": w_mfe_med,
            "long_mae": w_mae_p80, "short_mae": w_mae_p80,
        }

    def query_exit_profile(self, p_up: float, delta_p: float, accel: float,
                           now: datetime, current_unrealized_pct: float = 0.0,
                           hold_seconds: float = 0.0) -> dict:
        cutoff = now - timedelta(minutes=720)
        completed = [rec for rec in self.records
                     if rec.timestamp >= cutoff and rec.filled_15m]
        n_total = len(completed)

        result = {
            "n": 0, "has_data": False,
            "mfe_p25": 0.0, "mfe_p40": 0.0, "mfe_p50": 0.0,
            "mfe_p65": 0.0, "mfe_p75": 0.0, "mfe_p90": 0.0,
            "mae_p25": 0.0, "mae_p50": 0.0, "mae_p60": 0.0,
            "mae_p75": 0.0, "mae_p90": 0.0,
            "mfe_time_median": 300.0, "mfe_time_p75": 600.0,
            "prob_exceed_current": 0.5,
            "expected_additional_mfe": 0.0,
        }

        if n_total < 5:
            return result

        bw_scale = max(0.5, min(2.0, 50.0 / max(n_total, 1)))
        bw_pup = 0.10 * bw_scale
        bw_dp = 0.05 * bw_scale

        mfe_list = []
        mae_list = []
        mfe_time_list = []
        weights = []

        for rec in completed:
            elapsed_min = (now - rec.timestamp).total_seconds() / 60.0
            time_w = math.exp(-self.decay_lambda * elapsed_min)
            d_pup = (rec.p_up - p_up) / bw_pup
            d_dp = (rec.delta_p - delta_p) / bw_dp
            kernel_w = math.exp(-0.5 * (d_pup**2 + d_dp**2))
            w = time_w * kernel_w
            if w < 0.001:
                continue

            weights.append(w)
            mfe_list.append(rec.max_favorable if rec.max_favorable is not None else 0.0)
            mae_list.append(rec.max_adverse if rec.max_adverse is not None else 0.0)
            if rec.mfe_reached_at is not None:
                mfe_time_list.append(rec.mfe_reached_at)

        n = len(weights)
        if n < 5:
            return result

        w_arr = np.array(weights)
        mfe_arr = np.array(mfe_list[:n])
        mae_arr = np.array(mae_list[:n])

        def weighted_percentile(values, w, pct):
            idx = np.argsort(values)
            sorted_v = values[idx]
            sorted_w = w[idx]
            cum = np.cumsum(sorted_w)
            if cum[-1] == 0:
                return 0.0
            target = cum[-1] * pct / 100.0
            pos = np.searchsorted(cum, target)
            pos = min(pos, len(sorted_v) - 1)
            return float(sorted_v[pos])

        result["n"] = n
        result["has_data"] = True

        result["mfe_p25"] = weighted_percentile(mfe_arr, w_arr, 25)
        result["mfe_p40"] = weighted_percentile(mfe_arr, w_arr, 40)
        result["mfe_p50"] = weighted_percentile(mfe_arr, w_arr, 50)
        result["mfe_p65"] = weighted_percentile(mfe_arr, w_arr, 65)
        result["mfe_p75"] = weighted_percentile(mfe_arr, w_arr, 75)
        result["mfe_p90"] = weighted_percentile(mfe_arr, w_arr, 90)

        result["mae_p25"] = weighted_percentile(mae_arr, w_arr, 25)
        result["mae_p50"] = weighted_percentile(mae_arr, w_arr, 50)
        result["mae_p60"] = weighted_percentile(mae_arr, w_arr, 60)
        result["mae_p75"] = weighted_percentile(mae_arr, w_arr, 75)
        result["mae_p90"] = weighted_percentile(mae_arr, w_arr, 90)

        if mfe_time_list:
            mfe_t = np.array(mfe_time_list)
            result["mfe_time_median"] = float(np.median(mfe_t))
            result["mfe_time_p75"] = float(np.percentile(mfe_t, 75))

        if current_unrealized_pct > 0 and n >= 10:
            exceed_w = np.sum(w_arr[mfe_arr > current_unrealized_pct])
            result["prob_exceed_current"] = float(exceed_w / np.sum(w_arr)) if np.sum(w_arr) > 0 else 0.5
            exceeding_mask = mfe_arr > current_unrealized_pct
            if np.any(exceeding_mask):
                exc_vals = mfe_arr[exceeding_mask]
                exc_w = w_arr[exceeding_mask]
                result["expected_additional_mfe"] = float(
                    np.sum(exc_w * (exc_vals - current_unrealized_pct)) / np.sum(exc_w)
                )
        elif current_unrealized_pct <= 0:
            pos_w = np.sum(w_arr[mfe_arr > 0])
            result["prob_exceed_current"] = float(pos_w / np.sum(w_arr)) if np.sum(w_arr) > 0 else 0.5

        return result

    def get_adaptive_trail_width(self, p_up: float, delta_p: float,
                                  accel: float, sync_score: int,
                                  atr_val: float, now: datetime) -> float:
        base_width = atr_val * 1.5

        if accel > TREND_EXTEND_ACCEL_MIN:
            momentum_mult = TRAIL_ACCEL_WIDE
        elif accel < -TREND_EXTEND_ACCEL_MIN:
            momentum_mult = TRAIL_ACCEL_TIGHT
        else:
            momentum_mult = TRAIL_ACCEL_NORMAL

        abs_sync = abs(sync_score)
        if abs_sync >= TREND_EXTEND_SYNC_MIN:
            sync_mult = 1.3
        elif abs_sync >= 2:
            sync_mult = 1.0
        else:
            sync_mult = 0.7

        exit_profile = self.query_exit_profile(p_up, delta_p, accel, now)
        if exit_profile["has_data"]:
            if exit_profile["mae_p60"] > 0:
                stat_ratio = (exit_profile["mae_p60"] / 100.0 * atr_val * 100) / max(atr_val, 1e-8)
                stat_ratio = float(np.clip(stat_ratio, 0.5, 2.0))
                base_width *= stat_ratio

        trail_width = base_width * momentum_mult * sync_mult
        trail_width = float(np.clip(trail_width, atr_val * 0.8, atr_val * 5.0))
        return trail_width


# ============================================================
# 모멘텀 가속도 트래커
# ============================================================
@dataclass
class MomentumTracker:
    p_up_history: deque = field(default_factory=lambda: deque(maxlen=20))
    speed_history: deque = field(default_factory=lambda: deque(maxlen=10))

    def update(self, p_up: float) -> Tuple[float, float, float]:
        self.p_up_history.append(p_up)
        if len(self.p_up_history) < 2:
            return 0.0, 0.0, 0.0

        delta_p = self.p_up_history[-1] - self.p_up_history[-2]

        deltas = []
        hist = list(self.p_up_history)
        for i in range(1, min(4, len(hist))):
            deltas.append(hist[-i] - hist[-i-1] if i < len(hist) else 0)
        speed = np.mean(deltas) if deltas else 0.0

        self.speed_history.append(speed)
        if len(self.speed_history) >= 2:
            acceleration = self.speed_history[-1] - self.speed_history[-2]
        else:
            acceleration = 0.0

        return float(delta_p), float(speed), float(acceleration)


# ============================================================
# 멀티 타임프레임 동조 감지
# ============================================================
def calc_sync_score(tf_data: dict, prev_tf_data: Optional[dict]) -> int:
    if prev_tf_data is None:
        return 0
    score = 0
    for key in ["1m", "15m", "1h", "4h"]:
        curr = tf_data[key]["score"]
        prev = prev_tf_data.get(key, {}).get("score", curr)
        delta = curr - prev
        if delta > 0:
            score += 1
        elif delta < 0:
            score -= 1
    return score


# ============================================================
# 🆕 v7: 평균 회귀 트래커 (재입학 전략 핵심)
# ============================================================
@dataclass
class ReversionState:
    """개별 이탈-회귀 이벤트"""
    side: str                              # 회귀 방향: "LONG"(아래서 복귀) / "SHORT"(위에서 복귀)
    deviation_start_time: datetime = None   # 이탈 시작 시점
    deviation_peak_p_up: float = 0.5       # 이탈 극값 p_up
    deviation_peak_price: float = 0.0      # 이탈 극값 시점 가격
    reversion_started: bool = False         # 회귀 시작됐는지
    reversion_confirm_count: int = 0       # 연속 회귀 방향 사이클 수
    reversion_start_price: float = 0.0     # 회귀 시작 시점 가격
    deviation_distance: float = 0.0        # |p_up - 0.5| 최대 이탈 거리
    expired: bool = False                   # 만료됨 (사용 완료 또는 시간 초과)


@dataclass
class MeanReversionTracker:
    """
    재입학 회귀 전략의 핵심 엔진.

    원리:
    - 가격이 평균(p_up=0.5)에서 벗어날 때 → 기록만 함 (진입 X)
    - 가격이 다시 돌아오기 시작할 때 → 진입 (회귀 확인)
    - "벗어날 때는 어디까지 갈지 모르지만, 돌아오는 방향은 명확"

    상태 머신:
    IDLE → DEVIATED (이탈 감지) → REVERTING (회귀 시작) → CONFIRMED (진입 가능)
    """
    # 현재 추적 중인 이탈-회귀 이벤트
    active_states: List[ReversionState] = field(default_factory=list)

    # p_up 히스토리 (회귀 판단용)
    p_up_history: deque = field(default_factory=lambda: deque(maxlen=30))
    price_history: deque = field(default_factory=lambda: deque(maxlen=30))

    # 승률 추적 (회귀 전략 전용)
    reversion_wins: int = 0
    reversion_losses: int = 0
    reversion_total: int = 0
    recent_reversion_results: deque = field(default_factory=lambda: deque(maxlen=30))

    # 상태
    is_paused: bool = False  # 승률 저조 시 일시 중단

    def update(self, p_up: float, price: float, delta_p: float,
               accel: float, now: datetime) -> Optional[dict]:
        """
        매 사이클 호출. 이탈/회귀 상태를 업데이트하고,
        진입 가능한 회귀 신호가 있으면 반환.

        Returns:
            None: 신호 없음
            dict: {side, confidence, stop_price, tp1_price, tp2_price,
                   deviation_distance, reason}
        """
        self.p_up_history.append(p_up)
        self.price_history.append(price)

        # 승률 체크 → 일시 중단 판단
        self._check_pause()

        # 만료된 이벤트 정리 (5분 이상 지난 미확인 이탈)
        self.active_states = [
            s for s in self.active_states
            if not s.expired and (now - s.deviation_start_time).total_seconds() < 600
        ]

        # === 1단계: 새로운 이탈 감지 ===
        deviation_from_mean = p_up - 0.5

        if abs(deviation_from_mean) >= REVERSION_DEVIATION_THRESHOLD:
            # 이미 같은 방향 이탈이 추적 중인지 확인
            reversion_side = "LONG" if deviation_from_mean < 0 else "SHORT"
            already_tracking = any(
                s.side == reversion_side and not s.expired
                for s in self.active_states
            )

            if not already_tracking:
                # 새 이탈 이벤트 생성
                state = ReversionState(
                    side=reversion_side,
                    deviation_start_time=now,
                    deviation_peak_p_up=p_up,
                    deviation_peak_price=price,
                    deviation_distance=abs(deviation_from_mean),
                )
                self.active_states.append(state)

        # === 2단계: 기존 이탈의 극값 업데이트 + 회귀 감지 ===
        best_signal = None

        for state in self.active_states:
            if state.expired:
                continue

            # 극값 업데이트 (더 많이 이탈하면 갱신)
            if state.side == "LONG":
                # 아래로 이탈 중 → p_up이 더 낮아지면 극값 갱신
                if p_up < state.deviation_peak_p_up:
                    state.deviation_peak_p_up = p_up
                    state.deviation_peak_price = price
                    state.deviation_distance = abs(p_up - 0.5)
                    state.reversion_started = False
                    state.reversion_confirm_count = 0
            else:
                # 위로 이탈 중 → p_up이 더 높아지면 극값 갱신
                if p_up > state.deviation_peak_p_up:
                    state.deviation_peak_p_up = p_up
                    state.deviation_peak_price = price
                    state.deviation_distance = abs(p_up - 0.5)
                    state.reversion_started = False
                    state.reversion_confirm_count = 0

            # === 회귀 시작 감지 ===
            # delta_p가 평균(0.5) 방향으로 전환
            is_reverting = False

            if state.side == "LONG":
                # 아래서 올라오는 중: delta_p > 0 (p_up 증가)
                if delta_p >= REVERSION_CONFIRM_DELTA_P:
                    is_reverting = True
            else:
                # 위에서 내려오는 중: delta_p < 0 (p_up 감소)
                if delta_p <= -REVERSION_CONFIRM_DELTA_P:
                    is_reverting = True

            if is_reverting:
                if not state.reversion_started:
                    state.reversion_started = True
                    state.reversion_start_price = price
                    state.reversion_confirm_count = 1
                else:
                    state.reversion_confirm_count += 1
            else:
                # 회귀가 중단됨 → 카운터 리셋 (단, 0으로 완전 리셋은 아님)
                if state.reversion_started:
                    state.reversion_confirm_count = max(0, state.reversion_confirm_count - 1)
                    if state.reversion_confirm_count == 0:
                        state.reversion_started = False

            # === 3단계: 회귀 확인 → 진입 신호 ===
            required_cycles = REVERSION_CONFIRM_CYCLES

            # 승률 저조 시 더 강한 확인 요구
            current_wr = self.get_winrate()
            if current_wr is not None and current_wr < REVERSION_WARN_WINRATE:
                required_cycles = REVERSION_STRONG_CONFIRM_CYCLES

            if state.reversion_confirm_count >= required_cycles and not self.is_paused:
                # 가속도도 회귀 방향인지 확인 (보너스)
                accel_aligned = False
                if state.side == "LONG" and accel > REVERSION_ACCEL_BONUS:
                    accel_aligned = True
                elif state.side == "SHORT" and accel < -REVERSION_ACCEL_BONUS:
                    accel_aligned = True

                # 신뢰도 계산
                confidence = 0.5
                confidence += min(state.deviation_distance / 0.3, 0.25)  # 이탈이 클수록 회귀 확률 높음
                confidence += 0.1 if state.reversion_confirm_count >= REVERSION_STRONG_CONFIRM_CYCLES else 0.0
                confidence += 0.1 if accel_aligned else 0.0
                confidence = min(confidence, 1.0)

                # 손절가: 이탈 극값 바깥 (짧고 명확)
                if state.side == "LONG":
                    stop_price = state.deviation_peak_price * (1 - REVERSION_SL_BUFFER_PCT / 100)
                else:
                    stop_price = state.deviation_peak_price * (1 + REVERSION_SL_BUFFER_PCT / 100)

                # 익절 목표: 이탈 거리 기반 (보수적)
                if state.side == "LONG":
                    deviation_price_dist = price - state.deviation_peak_price
                    tp1_price = price + abs(deviation_price_dist) * REVERSION_TP1_RATIO
                    tp2_price = price + abs(deviation_price_dist) * REVERSION_TP2_RATIO
                else:
                    deviation_price_dist = state.deviation_peak_price - price
                    tp1_price = price - abs(deviation_price_dist) * REVERSION_TP1_RATIO
                    tp2_price = price - abs(deviation_price_dist) * REVERSION_TP2_RATIO

                signal = {
                    "side": state.side,
                    "confidence": confidence,
                    "stop_price": stop_price,
                    "tp1_price": tp1_price,
                    "tp2_price": tp2_price,
                    "deviation_distance": state.deviation_distance,
                    "deviation_peak_price": state.deviation_peak_price,
                    "confirm_cycles": state.reversion_confirm_count,
                    "accel_aligned": accel_aligned,
                    "reason": (
                        f"[회귀] {state.side} | 이탈={state.deviation_distance:.3f} "
                        f"확인={state.reversion_confirm_count}사이클 "
                        f"가속={'✓' if accel_aligned else '✗'} "
                        f"극값가격={state.deviation_peak_price:,.1f}"
                    ),
                }

                # 가장 좋은 신호 선택 (이탈 거리가 큰 것 우선)
                if best_signal is None or signal["deviation_distance"] > best_signal["deviation_distance"]:
                    best_signal = signal

                # 이 이벤트 만료 처리 (진입 신호 발생했으므로)
                state.expired = True

        return best_signal

    def record_trade_result(self, win: bool):
        """회귀 전략 거래 결과 기록"""
        self.reversion_total += 1
        if win:
            self.reversion_wins += 1
        else:
            self.reversion_losses += 1
        self.recent_reversion_results.append(win)

    def get_winrate(self) -> Optional[float]:
        """회귀 전략 승률 (최소 거래 수 미달이면 None)"""
        if self.reversion_total < REVERSION_MIN_TRADES:
            return None
        # 최근 거래 기반
        if len(self.recent_reversion_results) >= REVERSION_MIN_TRADES:
            recent = list(self.recent_reversion_results)
            return sum(1 for w in recent if w) / len(recent) * 100
        return self.reversion_wins / self.reversion_total * 100

    def _check_pause(self):
        """승률 기반 일시 중단 체크"""
        wr = self.get_winrate()
        if wr is None:
            self.is_paused = False
            return

        if wr < REVERSION_PAUSE_WINRATE:
            if not self.is_paused:
                self.is_paused = True
        elif wr >= REVERSION_TARGET_WINRATE:
            if self.is_paused:
                self.is_paused = False

    def get_status(self) -> str:
        """현재 상태 요약"""
        wr = self.get_winrate()
        active_count = sum(1 for s in self.active_states if not s.expired)
        reverting_count = sum(1 for s in self.active_states if not s.expired and s.reversion_started)

        wr_str = f"{wr:.0f}%" if wr is not None else "N/A"
        pause_str = " ⛔중단" if self.is_paused else ""

        return (
            f"회귀[이탈{active_count}/복귀중{reverting_count} "
            f"WR={wr_str}({self.reversion_total}건){pause_str}]"
        )


# ============================================================
# [lab1 융합] Q-Pulse + BB 역추세 트래커
# ============================================================
@dataclass
class QpulseSetupTracker:
    """
    lab1 Q-Pulse + BB 역추세 전략 트래커.

    [원리]
    - EMA(19) 상향교차 + 직전 QPULSE_MAX_LOOKBACK 캔들 내 Q-Pulse=1
      -> WAIT_BBU 상태 -> Close > BB_upper -> SHORT 진입
    - EMA(19) 하향교차 + 직전 QPULSE_MAX_LOOKBACK 캔들 내 Q-Pulse=-1
      -> WAIT_BBL 상태 -> Close < BB_lower -> LONG 진입
    - 반대 Setup 발생 시 이전 대기 상태 무효화

    신호 발생 시 반환 dict:
      {side, bb_middle, reason}
    """
    qpulse_history: deque = field(default_factory=lambda: deque(maxlen=20))
    state: Optional[str] = None   # None | 'WAIT_BBU' | 'WAIT_BBL'

    # 승률 추적
    total_signals: int = 0
    wins: int = 0

    def update(self, ema19: float, ema40: float,
               ema19_prev: float, ema40_prev: float,
               qpulse: int, close: float,
               bb_upper: float, bb_lower: float,
               bb_middle: float) -> Optional[dict]:
        """매 사이클 호출. 신호 발생 시 dict 반환, 없으면 None."""
        self.qpulse_history.append(qpulse)

        # EMA 교차 감지
        cross_up   = (ema19 > ema40) and (ema19_prev <= ema40_prev)
        cross_down = (ema19 < ema40) and (ema19_prev >= ema40_prev)

        # 최근 lookback 내 Q-Pulse 확인
        recent = list(self.qpulse_history)[-QPULSE_MAX_LOOKBACK:]
        has_bull = 1  in recent
        has_bear = -1 in recent

        # EMA 교차 시 역추세 대기 상태 설정 (즉시 진입 X — 추세 신호 제거)
        if cross_up:
            self.state = 'WAIT_BBU'
        elif cross_down:
            self.state = 'WAIT_BBL'

        # ── 역추세 신호: Setup 후 BB 실제 돌파 → 반대 방향 진입 ──
        if self.state == 'WAIT_BBU' and close >= bb_upper:
            self.state = None
            self.total_signals += 1
            return {
                "side": "SHORT",
                "signal_type": "reversal",
                "bb_upper": bb_upper, "bb_middle": bb_middle, "bb_lower": bb_lower,
                "reason": (
                    f"[QPulse-Rev] Long Setup + Close({close:,.1f}) >= "
                    f"BB상단({bb_upper:,.1f}) -> SHORT"
                ),
            }
        if self.state == 'WAIT_BBL' and close <= bb_lower:
            self.state = None
            self.total_signals += 1
            return {
                "side": "LONG",
                "signal_type": "reversal",
                "bb_upper": bb_upper, "bb_middle": bb_middle, "bb_lower": bb_lower,
                "reason": (
                    f"[QPulse-Rev] Short Setup + Close({close:,.1f}) <= "
                    f"BB하단({bb_lower:,.1f}) -> LONG"
                ),
            }
        return None

    def record_result(self, win: bool):
        if win:
            self.wins += 1

    def get_winrate(self) -> Optional[float]:
        if self.total_signals < QPULSE_MIN_TRADES:
            return None
        return self.wins / self.total_signals * 100

    def get_status(self) -> str:
        wr = self.get_winrate()
        state_str = self.state if self.state else "대기"
        wr_str = f"{wr:.0f}%" if wr is not None else "N/A"
        return f"QPulse[{state_str} WR={wr_str}({self.total_signals}건)]"


# ============================================================
# [v8] Q-Pulse Armed 회귀 트래커
# ============================================================
@dataclass
class QPulseArmedReversionTracker:
    """
    멀티TF Q-Pulse 신호를 게이트로 사용하는 강화된 회귀 전략.

    흐름:
    IDLE
      → 멀티TF Q-Pulse 가중합 임계값 초과
    ARMED (side=진입방향, armed_price=기준가 기록)
      → 가격이 Q-Pulse 방향으로 MIN_EXTEND_PCT% 이상 추가 진행
    EXTENDED (peak_price 확정)
      → delta_p 반전 + CONFIRM_CYCLES 연속 확인
    신호 반환 → MeanReversionTracker와 동일 포맷
               단, confidence 부스트 + 손절/익절은 극값 기반

    핵심 차이점:
    - 일반 회귀: p_up 수학적 이탈 기반
    - QPulse 회귀: 실제 강한 방향성 캔들(멀티TF 합의) 후 되돌림 기반
    """
    state: str = "IDLE"              # IDLE / ARMED / EXTENDED
    armed_side: str = ""             # "LONG"(아래서 복귀) / "SHORT"(위에서 복귀)
    armed_price: float = 0.0         # ARMED 전환 시점 가격
    armed_time: Optional[datetime] = None
    armed_qscore: float = 0.0        # ARMED 시점 Q-Pulse 스코어 강도

    peak_price: float = 0.0          # 가격 극값 (손절 기준)
    reversion_confirm_count: int = 0

    # 승률 추적
    wins: int = 0
    losses: int = 0
    total: int = 0
    recent_results: deque = field(default_factory=lambda: deque(maxlen=30))
    is_paused: bool = False

    def update(self, qpulse_score: float, price: float, delta_p: float,
               now: datetime) -> Optional[dict]:
        """
        매 사이클 호출. 진입 가능한 신호가 있으면 dict 반환.
        """
        self._check_pause()

        # === IDLE: ARMED 조건 체크 ===
        if self.state == "IDLE":
            abs_score = abs(qpulse_score)
            if abs_score >= QPULSE_MTF_ARMED_THRESHOLD:
                # Q-Pulse 강한 상승 → 나중에 SHORT 회귀 노림
                # Q-Pulse 강한 하락 → 나중에 LONG 회귀 노림
                self.armed_side = "SHORT" if qpulse_score > 0 else "LONG"
                self.armed_price = price
                self.armed_time = now
                self.armed_qscore = qpulse_score
                self.peak_price = price
                self.reversion_confirm_count = 0
                self.state = "ARMED"
            return None

        # === 만료 체크 ===
        if self.state in ("ARMED", "EXTENDED") and self.armed_time is not None:
            elapsed = (now - self.armed_time).total_seconds()
            if elapsed > QPULSE_MTF_ARMED_EXPIRE_SEC:
                self.state = "IDLE"
                return None

        # === ARMED: 가격 연장 감지 ===
        if self.state == "ARMED":
            # 새 Q-Pulse 강도가 더 세지면 armed_price 갱신 (더 최신 기준점)
            abs_score = abs(qpulse_score)
            if abs_score >= QPULSE_MTF_ARMED_THRESHOLD:
                new_side = "SHORT" if qpulse_score > 0 else "LONG"
                if new_side == self.armed_side:
                    self.armed_price = price
                    self.armed_time = now
                    self.armed_qscore = qpulse_score

            # 가격 극값 추적
            if self.armed_side == "SHORT":
                # 상승 펄스 → 가격이 더 올라가면 peak 갱신
                if price > self.peak_price:
                    self.peak_price = price
                # 연장 확인: peak가 armed_price 대비 MIN_EXTEND_PCT% 이상 올라감
                extend_pct = (self.peak_price - self.armed_price) / self.armed_price * 100
            else:
                # 하락 펄스 → 가격이 더 내려가면 peak 갱신
                if price < self.peak_price or self.peak_price == self.armed_price:
                    self.peak_price = price
                extend_pct = (self.armed_price - self.peak_price) / self.armed_price * 100

            if extend_pct >= QPULSE_MTF_MIN_EXTEND_PCT:
                self.state = "EXTENDED"
                self.reversion_confirm_count = 0

            return None

        # === EXTENDED: 회귀 방향 delta_p 확인 ===
        if self.state == "EXTENDED":
            # 극값 계속 추적
            if self.armed_side == "SHORT" and price > self.peak_price:
                self.peak_price = price
                self.reversion_confirm_count = 0
            elif self.armed_side == "LONG" and (price < self.peak_price or self.peak_price == 0):
                self.peak_price = price
                self.reversion_confirm_count = 0

            # 회귀 방향 delta_p 체크
            reverting = False
            if self.armed_side == "SHORT" and delta_p <= -QPULSE_MTF_CONFIRM_DELTA_P:
                reverting = True
            elif self.armed_side == "LONG" and delta_p >= QPULSE_MTF_CONFIRM_DELTA_P:
                reverting = True

            if reverting:
                self.reversion_confirm_count += 1
            else:
                self.reversion_confirm_count = max(0, self.reversion_confirm_count - 1)

            if self.reversion_confirm_count >= QPULSE_MTF_CONFIRM_CYCLES and not self.is_paused:
                signal = self._build_signal(price)
                self.state = "IDLE"
                return signal

        return None

    def _build_signal(self, price: float) -> dict:
        side = self.armed_side
        # 손절: 극값 바깥 버퍼
        if side == "LONG":
            stop_price = self.peak_price * (1 - QPULSE_MTF_SL_BUFFER_PCT / 100)
            dev_dist = abs(price - self.peak_price)
            tp1_price = price + dev_dist * QPULSE_MTF_TP1_RATIO
            tp2_price = price + dev_dist * QPULSE_MTF_TP2_RATIO
        else:
            stop_price = self.peak_price * (1 + QPULSE_MTF_SL_BUFFER_PCT / 100)
            dev_dist = abs(self.peak_price - price)
            tp1_price = price - dev_dist * QPULSE_MTF_TP1_RATIO
            tp2_price = price - dev_dist * QPULSE_MTF_TP2_RATIO

        # 신뢰도: Q-Pulse 스코어 강도 + 연장 거리로 부스트
        extend_pct = dev_dist / max(self.armed_price, 1) * 100
        confidence = QPULSE_MTF_BASE_CONFIDENCE
        confidence += min(abs(self.armed_qscore) / 1.0, 0.15)   # 스코어 강도 보너스
        confidence += min(extend_pct / 0.5, 0.10)               # 연장 거리 보너스
        confidence = min(confidence, 1.0)

        return {
            "side": side,
            "confidence": confidence,
            "stop_price": stop_price,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "deviation_peak_price": self.peak_price,
            "armed_qscore": self.armed_qscore,
            "confirm_cycles": self.reversion_confirm_count,
            "reason": (
                f"[QPulse-Armed 회귀] {side} | "
                f"멀티TF Q-Score={self.armed_qscore:+.2f} "
                f"극값={self.peak_price:,.1f} "
                f"연장={extend_pct:.2f}% "
                f"신뢰={confidence:.2f}"
            ),
        }

    def record_trade_result(self, win: bool):
        self.total += 1
        if win:
            self.wins += 1
        else:
            self.losses += 1
        self.recent_results.append(win)

    def get_winrate(self) -> Optional[float]:
        if self.total < QPULSE_MTF_MIN_TRADES:
            return None
        if len(self.recent_results) >= QPULSE_MTF_MIN_TRADES:
            recent = list(self.recent_results)
            return sum(1 for w in recent if w) / len(recent) * 100
        return self.wins / self.total * 100 if self.total > 0 else None

    def _check_pause(self):
        wr = self.get_winrate()
        if wr is None:
            self.is_paused = False
            return
        if wr < QPULSE_MTF_PAUSE_WINRATE:
            self.is_paused = True
        elif wr >= QPULSE_MTF_PAUSE_WINRATE + 10:
            self.is_paused = False

    def get_status(self) -> str:
        wr = self.get_winrate()
        wr_str = f"{wr:.0f}%" if wr is not None else "N/A"
        pause_str = " ⛔중단" if self.is_paused else ""
        return (
            f"QP-Armed[{self.state} side={self.armed_side or '-'} "
            f"WR={wr_str}({self.total}건){pause_str}]"
        )


# ============================================================
# 성과 피드백 루프
# ============================================================
@dataclass
class PerformanceFeedback:
    recent_pnls: deque = field(default_factory=lambda: deque(maxlen=FEEDBACK_WINDOW))

    def add_trade(self, pnl_pct: float):
        self.recent_pnls.append(pnl_pct)

    def get_aggression(self) -> Tuple[float, str]:
        if len(self.recent_pnls) < 3:
            return 1.0, "초기"
        cumulative = sum(self.recent_pnls)
        if cumulative > 1.0:
            return 1.2, "호조"
        elif cumulative > -1.0:
            return 1.0, "보통"
        elif cumulative > -3.0:
            return 0.6, "부진"
        else:
            return 0.3, "위기"


# ============================================================
# 트레일링 스탑 매니저
# ============================================================
@dataclass
class TrailingStopManager:
    entry_price: float = 0.0
    side: str = "LONG"
    initial_stop: float = 0.0
    current_stop: float = 0.0
    peak_price: float = 0.0
    atr_val: float = 0.0
    adaptive_trail_width: float = 0.0
    trend_extend_active: bool = False

    def init(self, entry_price: float, side: str, initial_stop: float, atr_val: float):
        self.entry_price = entry_price
        self.side = side
        self.initial_stop = initial_stop
        self.current_stop = initial_stop
        self.peak_price = entry_price
        self.atr_val = atr_val
        self.adaptive_trail_width = atr_val * 1.5
        self.trend_extend_active = False

    def set_adaptive_width(self, width: float):
        self.adaptive_trail_width = width

    def set_trend_extend(self, active: bool):
        self.trend_extend_active = active

    def update(self, current_price: float) -> float:
        if self.side == "LONG":
            self.peak_price = max(self.peak_price, current_price)
            profit_pct = (self.peak_price - self.entry_price) / self.entry_price * 100.0
        else:
            self.peak_price = min(self.peak_price, current_price)
            profit_pct = (self.entry_price - self.peak_price) / self.entry_price * 100.0

        trail = self.adaptive_trail_width
        if self.trend_extend_active:
            trail *= 1.3

        new_stop = self.initial_stop

        if profit_pct < 0.35:
            new_stop = self.initial_stop
        elif profit_pct < 0.8:
            if self.side == "LONG":
                new_stop = self.entry_price - trail * 0.3
            else:
                new_stop = self.entry_price + trail * 0.3
        elif profit_pct < 1.0:
            if self.side == "LONG":
                protected = self.entry_price + (self.peak_price - self.entry_price) * 0.40
                trail_stop = self.peak_price - trail
                new_stop = max(protected, trail_stop)
            else:
                protected = self.entry_price - (self.entry_price - self.peak_price) * 0.40
                trail_stop = self.peak_price + trail
                new_stop = min(protected, trail_stop)
        elif profit_pct < 2.0:
            if self.side == "LONG":
                protected = self.entry_price + (self.peak_price - self.entry_price) * 0.60
                trail_stop = self.peak_price - trail * 0.7
                new_stop = max(protected, trail_stop)
            else:
                protected = self.entry_price - (self.entry_price - self.peak_price) * 0.60
                trail_stop = self.peak_price + trail * 0.7
                new_stop = min(protected, trail_stop)
        else:
            if self.side == "LONG":
                protected = self.entry_price + (self.peak_price - self.entry_price) * 0.75
                trail_stop = self.peak_price - trail * 0.5
                new_stop = max(protected, trail_stop)
            else:
                protected = self.entry_price - (self.entry_price - self.peak_price) * 0.75
                trail_stop = self.peak_price + trail * 0.5
                new_stop = min(protected, trail_stop)

        if self.side == "LONG":
            self.current_stop = max(self.current_stop, new_stop)
        else:
            if self.current_stop == 0:
                self.current_stop = new_stop
            else:
                self.current_stop = min(self.current_stop, new_stop)

        return self.current_stop

    def force_breakeven(self):
        if self.side == "LONG":
            self.current_stop = max(self.current_stop, self.entry_price)
        else:
            self.current_stop = min(self.current_stop, self.entry_price) if self.current_stop > 0 else self.entry_price

    def force_tighten(self, pct_protect: float):
        if self.side == "LONG":
            protected = self.entry_price + (self.peak_price - self.entry_price) * pct_protect
            self.current_stop = max(self.current_stop, protected)
        else:
            protected = self.entry_price - (self.entry_price - self.peak_price) * pct_protect
            self.current_stop = min(self.current_stop, protected) if self.current_stop > 0 else protected

    def is_hit(self, current_price: float) -> bool:
        if self.side == "LONG":
            return current_price <= self.current_stop
        else:
            return current_price >= self.current_stop


# ============================================================
# 분할 포지션 매니저
# ============================================================
@dataclass
class SplitEntry:
    price: float
    qty: float
    time: datetime
    fees: float


@dataclass
class SplitPosition:
    side: str
    strategy: str
    signal_id: int = 0
    trailing: TrailingStopManager = field(default_factory=TrailingStopManager)
    entries: List[SplitEntry] = field(default_factory=list)
    entry_phase: int = 0
    exit_phase: int = 0
    total_qty: float = 0.0
    remaining_qty: float = 0.0
    avg_entry_price: float = 0.0
    total_fees: float = 0.0
    entry_start_time: Optional[datetime] = None
    initial_stop: float = 0.0
    best_price_since_entry: Optional[float] = None
    confirmed_direction: bool = False
    cycles_held: int = 0
    worst_unrealized: float = 0.0
    best_unrealized: float = 0.0
    partial_realized_pnl: float = 0.0

    # v7: 회귀 전략 메타데이터
    reversion_deviation_peak_price: float = 0.0  # 이탈 극값 가격
    reversion_tp1_price: float = 0.0             # 1차 익절 목표
    reversion_tp2_price: float = 0.0             # 2차 익절 목표

    # [lab1 융합] Q-Pulse BB 역추세 메타데이터
    qpulse_bb_middle: float = 0.0                # BB 중심선 (1차 익절 목표)

    # [v8] Q-Pulse Armed 회귀 메타데이터
    qp_armed_peak_price: float = 0.0             # 극값 가격 (손절 기준)
    qp_armed_tp1_price: float = 0.0              # 1차 익절
    qp_armed_tp2_price: float = 0.0              # 2차 익절

    def add_entry(self, price: float, qty: float, time: datetime, fees: float):
        self.entries.append(SplitEntry(price=price, qty=qty, time=time, fees=fees))
        self.total_fees += fees
        old_notional = self.avg_entry_price * self.total_qty
        new_notional = price * qty
        self.total_qty += qty
        self.remaining_qty += qty
        self.avg_entry_price = (old_notional + new_notional) / self.total_qty if self.total_qty > 0 else price
        self.entry_phase += 1
        if self.entry_start_time is None:
            self.entry_start_time = time

    def partial_exit(self, exit_qty: float, exit_price: float, fees: float) -> float:
        if self.side == "LONG":
            pnl = (exit_price - self.avg_entry_price) * exit_qty
        else:
            pnl = (self.avg_entry_price - exit_price) * exit_qty
        self.remaining_qty -= exit_qty
        self.total_fees += fees
        self.exit_phase += 1
        self.partial_realized_pnl += pnl
        return pnl

    def tick(self, current_price: float):
        self.cycles_held += 1
        ur = self.unrealized_pnl_pct(current_price)
        self.worst_unrealized = min(self.worst_unrealized, ur)
        self.best_unrealized = max(self.best_unrealized, ur)

    def hold_seconds(self, now: datetime) -> float:
        if self.entry_start_time is None:
            return 0.0
        return (now - self.entry_start_time).total_seconds()

    def should_add_entry(self, current_price: float, delta_p: float) -> bool:
        if self.entry_phase >= 3:
            return False
        unrealized = self.unrealized_pnl_pct(current_price)
        if unrealized < -0.05:
            return False

        if self.entry_phase == 1:
            if self.best_price_since_entry is None:
                self.best_price_since_entry = current_price
            if self.side == "LONG":
                self.best_price_since_entry = max(self.best_price_since_entry, current_price)
                move_pct = (self.best_price_since_entry - self.entries[0].price) / self.entries[0].price * 100
            else:
                self.best_price_since_entry = min(self.best_price_since_entry, current_price)
                move_pct = (self.entries[0].price - self.best_price_since_entry) / self.entries[0].price * 100
            return move_pct >= SPLIT_ENTRY_CONFIRM_PCT

        elif self.entry_phase == 2:
            if self.side == "LONG":
                return delta_p > 0.02 and unrealized >= 0
            else:
                return delta_p < -0.02 and unrealized >= 0

        return False

    def unrealized_pnl_pct(self, current_price: float) -> float:
        if self.total_qty == 0:
            return 0.0
        if self.side == "LONG":
            return (current_price - self.avg_entry_price) / self.avg_entry_price * 100
        else:
            return (self.avg_entry_price - current_price) / self.avg_entry_price * 100


# ============================================================
# 성과 추적
# ============================================================
@dataclass
class Trade:
    entry_time: datetime
    exit_time: datetime
    side: str
    strategy: str
    entry_price: float
    exit_price: float
    pnl: float
    exit_reason: str
    hour: int


@dataclass
class Performance:
    balance: float = START_BALANCE
    realized_pnl: float = 0.0
    peak_balance: float = START_BALANCE

    long_pnl: float = 0.0
    short_pnl: float = 0.0
    trend_pnl: float = 0.0
    range_pnl: float = 0.0
    reversion_pnl: float = 0.0      # v7: 회귀 전략 PnL
    qpulse_pnl: float = 0.0        # [lab1 융합] Q-Pulse BB 전략 PnL
    qp_armed_rev_pnl: float = 0.0  # [v8] Q-Pulse Armed 회귀 PnL

    trades: int = 0
    long_trades: int = 0
    short_trades: int = 0
    trend_trades: int = 0
    range_trades: int = 0
    reversion_trades: int = 0       # v7
    qpulse_trades: int = 0          # [lab1 융합]
    qp_armed_rev_trades: int = 0    # [v8]

    stoplosses: int = 0
    consecutive_losses: int = 0
    trade_history: deque = field(default_factory=lambda: deque(maxlen=200))
    hourly_pnl: dict = field(default_factory=lambda: {h: 0.0 for h in range(24)})
    hourly_trades: dict = field(default_factory=lambda: {h: 0 for h in range(24)})

    def total_return_pct(self) -> float:
        return (self.balance / START_BALANCE - 1.0) * 100.0

    def current_drawdown_pct(self) -> float:
        if self.peak_balance == 0:
            return 0.0
        return ((self.peak_balance - self.balance) / self.peak_balance) * 100.0

    def update_peak(self):
        if self.balance > self.peak_balance:
            self.peak_balance = self.balance

    def winrate(self) -> float:
        if self.trades == 0:
            return 0.0
        wins = sum(1 for t in self.trade_history if t.pnl > 0)
        return (wins / self.trades) * 100.0

    def profit_factor(self) -> float:
        if not self.trade_history:
            return 0.0
        gross_profit = sum(t.pnl for t in self.trade_history if t.pnl > 0)
        gross_loss = abs(sum(t.pnl for t in self.trade_history if t.pnl < 0))
        if gross_loss == 0:
            return 999.0 if gross_profit > 0 else 0.0
        return gross_profit / gross_loss

    def sharpe_ratio(self) -> float:
        if len(self.trade_history) < 5:
            return 0.0
        returns = [t.pnl / START_BALANCE for t in self.trade_history]
        mean_r = np.mean(returns)
        std_r = np.std(returns)
        if std_r == 0:
            return 0.0
        return (mean_r / std_r) * np.sqrt(10 * 365)

    def avg_win_loss_ratio(self) -> float:
        wins = [t.pnl for t in self.trade_history if t.pnl > 0]
        losses = [abs(t.pnl) for t in self.trade_history if t.pnl < 0]
        if not wins or not losses:
            return 0.0
        return np.mean(wins) / np.mean(losses)


# ============================================================
# 유틸
# ============================================================
def apply_slippage(price: float, side: str, is_entry: bool) -> float:
    slip = SLIPPAGE_BPS / 10000.0
    if side == "LONG":
        return price * (1 + slip) if is_entry else price * (1 - slip)
    else:
        return price * (1 - slip) if is_entry else price * (1 + slip)


def fee(notional: float) -> float:
    return notional * TAKER_FEE_RATE


def fmt_price(x) -> str:
    if x is None:
        return "-"
    return f"{x:,.1f}"


KST = timezone(timedelta(hours=9))

def fmt_time() -> str:
    return datetime.now(KST).strftime("%H:%M:%S")


def kelly_criterion(winrate_pct: float, avg_wl_ratio: float, base_ratio: float) -> float:
    wr = winrate_pct / 100.0
    if wr <= 0.0 or avg_wl_ratio <= 0.0:
        return base_ratio * 0.5
    kelly = (wr * avg_wl_ratio - (1 - wr)) / avg_wl_ratio
    kelly = float(np.clip(kelly, 0, 1))
    kelly_half = kelly * 0.5
    return base_ratio * (0.5 + kelly_half)


# ============================================================
# 종합 진입 판단 엔진
# ============================================================
@dataclass
class EntryDecision:
    should_enter: bool = False
    side: str = ""
    strategy: str = ""
    position_ratio: float = 0.0
    stop_price: float = 0.0
    tp_target_pct: float = 0.0
    reason: str = ""
    confidence_tier: str = "low"
    sync_score: int = 0

    # v7: 회귀 전략 메타데이터
    is_reversion: bool = False
    reversion_deviation_peak_price: float = 0.0
    reversion_tp1_price: float = 0.0
    reversion_tp2_price: float = 0.0

    # [lab1 융합] Q-Pulse BB 메타데이터
    is_qpulse_bb: bool = False
    qpulse_bb_middle: float = 0.0

    # [v8] Q-Pulse Armed 회귀 메타데이터
    is_qp_armed_rev: bool = False
    qp_armed_peak_price: float = 0.0
    qp_armed_tp1_price: float = 0.0
    qp_armed_tp2_price: float = 0.0


def _dynamic_stop_mult(atr_val: float, avg_atr: float, base_mult: float) -> float:
    if avg_atr == 0:
        return base_mult
    vol_ratio = atr_val / avg_atr
    return float(np.clip(base_mult * vol_ratio, 1.5, 3.5))


def decide_entry(
    p_up: float,
    delta_p: float,
    accel: float,
    sync_score: int,
    is_ranging: bool,
    range_signal: dict,
    stat_analyzer: StatAnalyzer,
    perf: Performance,
    feedback: PerformanceFeedback,
    reversion_tracker: MeanReversionTracker,
    qpulse_tracker: "QpulseSetupTracker",
    qpulse_signal: Optional[dict],
    qp_armed_tracker: "QPulseArmedReversionTracker",  # [v8]
    qp_armed_signal: Optional[dict],                  # [v8] 이번 사이클 Armed 신호
    now: datetime,
    last_price: float,
    atr15: float,
    avg_atr: float,
    tf: dict,
    last_exit_time: Optional[datetime] = None,
    last_exit_side: Optional[str] = None,
) -> EntryDecision:
    """
    v8: Q-Pulse Armed 회귀 전략 통합 진입 엔진.

    진입 우선순위:
    1. Q-Pulse Armed 회귀 (멀티TF 펄스 후 되돌림) — 최우선
    2. 일반 회귀 + QPULSE BB 융합/단독
    3. 커널 통계 + 회귀 필터 (ESS >= 8)
    4. 모멘텀 + 회귀 필터 (ESS < 8)
    """

    decision = EntryDecision()
    aggression_mult, aggression_label = feedback.get_aggression()

    # === 리스크 체크 ===
    dd = perf.current_drawdown_pct()
    if dd >= MAX_DRAWDOWN_PCT:
        return decision
    if perf.consecutive_losses >= MAX_CONSECUTIVE_LOSSES:
        return decision

    # === 재진입 쿨다운 ===
    if last_exit_time is not None:
        elapsed_since_exit = (now - last_exit_time).total_seconds()
        desired_side = "LONG" if p_up >= 0.5 else "SHORT"

        if last_exit_side == desired_side and elapsed_since_exit < REENTRY_SAME_DIR_COOLDOWN_SEC:
            return decision
        elif elapsed_since_exit < REENTRY_COOLDOWN_SEC:
            return decision

    # === 기본 Kelly 포지션 크기 ===
    if perf.trades >= 20:
        base_kelly = kelly_criterion(perf.winrate(), perf.avg_win_loss_ratio(), BASE_POSITION_RATIO)
    else:
        base_kelly = BASE_POSITION_RATIO * 0.7

    # =========================================
    # [v8] Q-Pulse Armed 회귀 — 최우선 처리
    # =========================================
    qp_armed = qp_armed_signal
    if qp_armed is not None and last_exit_time is not None:
        elapsed_qa = (now - last_exit_time).total_seconds()
        side_qa = qp_armed["side"]
        if (last_exit_side == side_qa and elapsed_qa < REENTRY_SAME_DIR_COOLDOWN_SEC) \
                or elapsed_qa < REENTRY_COOLDOWN_SEC:
            qp_armed = None

    if qp_armed is not None:
        side = qp_armed["side"]
        conf = qp_armed["confidence"]
        # 반대 방향으로 강한 커널 엣지가 있으면 취소
        scan_qa = stat_analyzer.scan_opportunity(p_up=p_up, delta_p=delta_p, accel=accel,
                                                  sync_score=sync_score, now=now)
        opp_edge_qa = scan_qa["short_edge"] if side == "LONG" else scan_qa["long_edge"]
        if opp_edge_qa > TOTAL_COST_PCT * 4 and scan_qa["ess"] >= 15:
            qp_armed = None  # 강한 반대 엣지 → 취소

    if qp_armed is not None:
        side = qp_armed["side"]
        conf = qp_armed["confidence"]
        pos_ratio = base_kelly * aggression_mult * conf * SPLIT_ENTRY_RATIOS[0] * 1.6
        pos_ratio = float(np.clip(pos_ratio, 0.05, 0.9))
        stop_dist_pct = abs(last_price - qp_armed["stop_price"]) / last_price * 100
        tier = "high" if conf >= 0.8 else "medium"
        decision.should_enter = True
        decision.side = side
        decision.strategy = "QPULSE_REVERSION"
        decision.position_ratio = pos_ratio
        decision.stop_price = qp_armed["stop_price"]
        decision.confidence_tier = tier
        decision.sync_score = sync_score
        decision.is_qp_armed_rev = True
        decision.qp_armed_peak_price = qp_armed["deviation_peak_price"]
        decision.qp_armed_tp1_price = qp_armed["tp1_price"]
        decision.qp_armed_tp2_price = qp_armed["tp2_price"]
        decision.tp_target_pct = stop_dist_pct * 2.5
        decision.reason = qp_armed["reason"] + f" | {aggression_label}"
        return decision

    # =========================================
    # REVERSION + QPULSE 융합 진입 엔진
    # =========================================
    reversion_signal = reversion_tracker.update(p_up, last_price, delta_p, accel, now)

    # 쿨다운 필터
    if reversion_signal is not None and last_exit_time is not None:
        elapsed_since_exit = (now - last_exit_time).total_seconds()
        side_rev = reversion_signal["side"]
        if (last_exit_side == side_rev and elapsed_since_exit < REENTRY_SAME_DIR_COOLDOWN_SEC) \
                or elapsed_since_exit < REENTRY_COOLDOWN_SEC:
            reversion_signal = None

    qp_valid = qpulse_signal
    if qp_valid is not None and last_exit_time is not None:
        elapsed_qp = (now - last_exit_time).total_seconds()
        side_qp = qp_valid["side"]
        if (last_exit_side == side_qp and elapsed_qp < REENTRY_SAME_DIR_COOLDOWN_SEC) \
                or elapsed_qp < REENTRY_COOLDOWN_SEC:
            qp_valid = None

    has_rev = reversion_signal is not None
    has_qp  = qp_valid is not None

    if has_rev or has_qp:
        both_same_dir = (
            has_rev and has_qp and
            reversion_signal["side"] == qp_valid["side"]
        )

        if both_same_dir:
            # 커널이 강하게 반대방향 → FUSION 취소, REVERSION 단독으로 처리
            side = reversion_signal["side"]
            scan = stat_analyzer.scan_opportunity(p_up=p_up, delta_p=delta_p, accel=accel,
                                                  sync_score=sync_score, now=now)
            opp_edge = scan["short_edge"] if side == "LONG" else scan["long_edge"]
            if opp_edge > TOTAL_COST_PCT * 3 and scan["ess"] >= 15:
                both_same_dir = False
                has_qp = False

        if both_same_dir:
            # ── FUSION: 두 전략 같은 방향 → 강한 진입 ──
            side = reversion_signal["side"]
            conf = reversion_signal["confidence"]
            pos_ratio = base_kelly * aggression_mult * conf * SPLIT_ENTRY_RATIOS[0] * 1.5
            pos_ratio = float(np.clip(pos_ratio, 0.05, 0.9))
            stop_dist_pct = abs(last_price - reversion_signal["stop_price"]) / last_price * 100
            decision.should_enter = True
            decision.side = side
            decision.strategy = "FUSION"
            decision.position_ratio = pos_ratio
            decision.stop_price = reversion_signal["stop_price"]
            decision.confidence_tier = "high"
            decision.sync_score = sync_score
            decision.is_reversion = True
            decision.reversion_deviation_peak_price = reversion_signal["deviation_peak_price"]
            decision.reversion_tp1_price = reversion_signal["tp1_price"]
            decision.reversion_tp2_price = reversion_signal["tp2_price"]
            decision.tp_target_pct = stop_dist_pct * 2.5
            decision.reason = (
                f"[FUSION] {reversion_signal['reason']} + {qp_valid['reason']} "
                f"| pos={pos_ratio:.0%} | {aggression_label}"
            )
            return decision

        # ── REVERSION 단독 ──
        if has_rev:
            side = reversion_signal["side"]
            conf = reversion_signal["confidence"]
            scan = stat_analyzer.scan_opportunity(p_up=p_up, delta_p=delta_p, accel=accel,
                                                  sync_score=sync_score, now=now)
            opp_edge = scan["short_edge"] if side == "LONG" else scan["long_edge"]
            if opp_edge <= TOTAL_COST_PCT * 3 or scan["ess"] < 15:
                pos_ratio = base_kelly * aggression_mult * conf * SPLIT_ENTRY_RATIOS[0]
                pos_ratio = float(np.clip(pos_ratio, 0.05, 0.8))
                tier = "high" if conf >= 0.8 else ("medium" if conf >= 0.6 else "low")
                if tier == "low":
                    pos_ratio = min(pos_ratio, base_kelly * 0.3)
                decision.should_enter = True
                decision.side = side
                decision.strategy = "REVERSION"
                decision.position_ratio = pos_ratio
                decision.stop_price = reversion_signal["stop_price"]
                decision.confidence_tier = tier
                decision.sync_score = sync_score
                decision.is_reversion = True
                decision.reversion_deviation_peak_price = reversion_signal["deviation_peak_price"]
                decision.reversion_tp1_price = reversion_signal["tp1_price"]
                decision.reversion_tp2_price = reversion_signal["tp2_price"]
                stop_dist_pct = abs(last_price - reversion_signal["stop_price"]) / last_price * 100
                decision.tp_target_pct = stop_dist_pct * 2.0
                decision.reason = reversion_signal["reason"] + f" | {aggression_label}"
                return decision

        # ── QPULSE 단독 ──
        if has_qp:
            side = qp_valid["side"]

            # [v8] 필터 1: BB 밴드 위치 — 외곽 EDGE_RATIO 영역에서만 허용
            bb_u_qp = qp_valid.get("bb_upper", 0.0)
            bb_l_qp = qp_valid.get("bb_lower", 0.0)
            bb_width = bb_u_qp - bb_l_qp
            if bb_width > 0:
                bb_pos = (last_price - bb_l_qp) / bb_width  # 0=하단, 1=상단
                if side == "LONG"  and bb_pos > QPULSE_BB_BAND_EDGE_RATIO:
                    return decision  # 중앙/상단 근처에서 LONG 거부
                if side == "SHORT" and bb_pos < (1.0 - QPULSE_BB_BAND_EDGE_RATIO):
                    return decision  # 중앙/하단 근처에서 SHORT 거부

            # [v8] 필터 2: 거래량 — 15m 평균 거래량 이상일 때만 허용
            vol_now   = tf["15m"].get("volume", 0.0)
            vol_avg   = tf["15m"].get("vol_ma20", 0.0)
            if vol_avg > 0 and vol_now < vol_avg * QPULSE_BB_VOL_MULT:
                return decision  # 거래량 부족 — 신뢰도 낮음

            # [v9] 필터 3: RSI 과매도/과매수 확인 — BB 역추세는 극단 RSI에서만 유효
            rsi_now = tf["15m"].get("rsi14", 50.0)
            if side == "LONG"  and rsi_now > QPULSE_BB_RSI_LONG_MAX:
                return decision  # 과매도 미확인 — 롱 역추세 거부
            if side == "SHORT" and rsi_now < QPULSE_BB_RSI_SHORT_MIN:
                return decision  # 과매수 미확인 — 숏 역추세 거부

            stop_dist = QPULSE_SL_ATR_MULT * atr15
            stop_price = last_price - stop_dist if side == "LONG" else last_price + stop_dist
            pos_ratio = base_kelly * aggression_mult * 0.5 * SPLIT_ENTRY_RATIOS[0]
            pos_ratio = float(np.clip(pos_ratio, 0.05, 0.6))
            decision.should_enter = True
            decision.side = side
            decision.strategy = "QPULSE_BB"
            decision.position_ratio = pos_ratio
            decision.stop_price = stop_price
            decision.confidence_tier = "medium"
            decision.sync_score = sync_score
            decision.is_qpulse_bb = True
            decision.qpulse_bb_middle = qp_valid["bb_middle"]
            decision.tp_target_pct = stop_dist / last_price * 100 * 1.5
            decision.reason = qp_valid["reason"] + f" | {aggression_label}"
            return decision

    # =========================================
    # 커널 회귀로 양방향 기대수익 스캔
    # =========================================
    scan = stat_analyzer.scan_opportunity(
        p_up=p_up, delta_p=delta_p, accel=accel,
        sync_score=sync_score, now=now,
    )

    # === 2순위: 통계 데이터 부족 시 — 모멘텀 + 회귀 필터 ===
    if scan["ess"] < 8 and (STRATEGY_ALLOW_TREND or STRATEGY_ALLOW_RANGE):
        score = 0.0
        score += (p_up - 0.5) * 4.0
        if abs(delta_p) > 0.01:
            score += np.sign(delta_p) * min(abs(delta_p) * 20, 1.5)
        if abs(accel) > 0.005:
            score += np.sign(accel) * 0.5
        score += sync_score * 0.3

        min_score = TREND_MIN_SCORE
        if abs(score) < min_score:
            return decision

        side = "LONG" if score > 0 else "SHORT"

        # ADX 필터: 추세 진입은 ADX >= TREND_MIN_ADX 이상일 때만
        adx_now = tf["15m"]["adx14"]
        if not is_ranging and adx_now < TREND_MIN_ADX:
            return decision

        # 방향성 필터: delta_p가 진입 방향과 너무 강하게 반대면 거부
        if side == "LONG" and delta_p < -0.03:
            return decision  # 강하게 하락 중 LONG 거부
        if side == "SHORT" and delta_p > 0.03:
            return decision  # 강하게 상승 중 SHORT 거부

        if is_ranging and range_signal["action"] in ["LONG", "SHORT"]:
            rsi_val = tf["15m"]["rsi14"]
            if range_signal["action"] == "LONG" and rsi_val < RANGE_RSI_OVERSOLD:
                side = "LONG"
            elif range_signal["action"] == "SHORT" and rsi_val > RANGE_RSI_OVERBOUGHT:
                side = "SHORT"

        pos_ratio = base_kelly * aggression_mult * 0.4 * SPLIT_ENTRY_RATIOS[0]
        pos_ratio = float(np.clip(pos_ratio, 0.05, 0.5))

        stop_mult = _dynamic_stop_mult(atr15, avg_atr, STOP_ATR_MULT)
        if side == "LONG":
            decision.stop_price = last_price - stop_mult * atr15
        else:
            decision.stop_price = last_price + stop_mult * atr15

        target_strategy = "RANGE" if is_ranging else "TREND"
        if target_strategy == "TREND" and not STRATEGY_ALLOW_TREND:
            return decision
        if target_strategy == "RANGE" and not STRATEGY_ALLOW_RANGE:
            return decision

        decision.should_enter = True
        decision.side = side
        decision.strategy = target_strategy
        decision.position_ratio = pos_ratio
        decision.confidence_tier = "low"
        decision.sync_score = sync_score
        decision.tp_target_pct = stop_mult * atr15 / last_price * 100 * 1.5
        decision.reason = (
            f"[모멘텀+회귀필터] {side} | 점수={score:+.1f} p_up={p_up:.2f} "
            f"Δp={delta_p:+.3f} 동조={sync_score} ESS={scan['ess']:.0f} {aggression_label}"
        )
        return decision

    # =========================================
    # 3순위: 통계 모드 — 커널 결과 + 회귀 필터
    # =========================================
    best_side = scan["best_side"]

    if best_side is None:
        return decision

    if best_side == "LONG":
        edge = scan["long_edge"]
        wr = scan["long_wr"]
    else:
        edge = scan["short_edge"]
        wr = scan["short_wr"]

    # 🆕 v7 회귀 필터: 통계 에지가 있어도 "돌아올 때만" 진입
    if len(reversion_tracker.p_up_history) >= 3:
        recent_pups = list(reversion_tracker.p_up_history)[-5:]
        if best_side == "LONG" and delta_p < -0.03:
            return decision  # 강하게 하락 중 LONG 거부
        if best_side == "SHORT" and delta_p > 0.03:
            return decision  # 강하게 상승 중 SHORT 거부

    # 동조 점수 보정
    sync_aligned = (
        (best_side == "LONG" and sync_score > 0) or
        (best_side == "SHORT" and sync_score < 0)
    )
    sync_opposed = (
        (best_side == "LONG" and sync_score < 0) or
        (best_side == "SHORT" and sync_score > 0)
    )

    if sync_opposed and abs(sync_score) >= 2 and edge < TOTAL_COST_PCT * 2:
        return decision

    # 포지션 크기
    edge_factor = min(edge / max(TOTAL_COST_PCT * 3, 0.01), 2.0)
    conf_factor = scan["confidence"]

    sync_mult = 1.0
    if sync_aligned and abs(sync_score) >= 2:
        sync_mult = 1.3
    elif sync_aligned:
        sync_mult = 1.1
    elif sync_opposed:
        sync_mult = 0.7

    pos_ratio = base_kelly * aggression_mult * edge_factor * conf_factor * sync_mult * SPLIT_ENTRY_RATIOS[0]
    pos_ratio = float(np.clip(pos_ratio, 0.05, 1.0))

    if scan["confidence"] >= 0.7 and edge > TOTAL_COST_PCT * 2:
        confidence = "high"
    elif scan["confidence"] >= 0.3 and edge > TOTAL_COST_PCT:
        confidence = "medium"
    else:
        confidence = "low"
        pos_ratio = min(pos_ratio, base_kelly * 0.3)

    # 손절가
    stop_mult = _dynamic_stop_mult(atr15, avg_atr, STOP_ATR_MULT)
    mae_val = scan["long_mae"] if best_side == "LONG" else scan["short_mae"]

    if mae_val > 0 and scan["ess"] >= 15:
        mae_stop_dist = last_price * mae_val / 100.0 * 1.3
        atr_stop_dist = stop_mult * atr15
        stat_weight = min(scan["ess"] / 50.0, 0.8)
        stop_dist = stat_weight * mae_stop_dist + (1 - stat_weight) * atr_stop_dist
    else:
        stop_dist = stop_mult * atr15

    if best_side == "LONG":
        decision.stop_price = last_price - stop_dist
    else:
        decision.stop_price = last_price + stop_dist

    # 익절 목표
    mfe_val = scan["long_mfe"] if best_side == "LONG" else scan["short_mfe"]
    if mfe_val > 0 and scan["ess"] >= 15:
        decision.tp_target_pct = mfe_val * 0.7
    else:
        decision.tp_target_pct = stop_mult * atr15 / last_price * 100 * 1.5

    stat_strategy = "RANGE" if is_ranging else "TREND"
    if stat_strategy == "TREND" and not STRATEGY_ALLOW_TREND:
        return decision
    if stat_strategy == "RANGE" and not STRATEGY_ALLOW_RANGE:
        return decision

    decision.strategy = stat_strategy
    decision.should_enter = True
    decision.side = best_side
    decision.position_ratio = pos_ratio
    decision.confidence_tier = confidence
    decision.sync_score = sync_score

    decision.reason = (
        f"[통계+회귀필터] {best_side} | 에지={edge:+.3f}% "
        f"L={scan['long_edge']:+.3f}/S={scan['short_edge']:+.3f} "
        f"WR={wr:.0f}% ESS={scan['ess']:.0f} "
        f"신뢰={scan['confidence']:.2f} 동조={sync_score} {aggression_label}"
    )

    return decision


# ============================================================
# 통계적 청산 판단
# ============================================================
def decide_exit(
    pos: SplitPosition,
    p_up: float,
    delta_p: float,
    accel: float,
    speed: float,
    sync_score: int,
    last_price: float,
    is_ranging: bool,
    range_signal: dict,
    stat_analyzer: StatAnalyzer,
    now: datetime,
    atr15: float,
) -> Tuple[Optional[str], float]:

    unrealized = pos.unrealized_pnl_pct(last_price)
    hold_sec = pos.hold_seconds(now)

    exit_profile = stat_analyzer.query_exit_profile(
        p_up, delta_p, accel, now,
        current_unrealized_pct=unrealized,
        hold_seconds=hold_sec,
    )
    has_stats = exit_profile["has_data"]

    trail_width = stat_analyzer.get_adaptive_trail_width(
        p_up, delta_p, accel, sync_score, atr15, now
    )
    pos.trailing.set_adaptive_width(trail_width)

    is_trend_extending = (
        abs(sync_score) >= TREND_EXTEND_SYNC_MIN and
        accel > TREND_EXTEND_ACCEL_MIN and
        unrealized > 0.3
    )
    pos.trailing.set_trend_extend(is_trend_extending)

    # === 🆕 v7: 회귀/융합 전략 전용 익절 ===
    if pos.strategy in ("REVERSION", "FUSION"):
        # 1차 회귀 익절: 이탈 거리 50% 회복
        if pos.exit_phase == 0 and unrealized > 0:
            if pos.side == "LONG" and last_price >= pos.reversion_tp1_price and pos.reversion_tp1_price > 0:
                return "회귀TP1(50%회복)", SPLIT_EXIT_RATIOS[0]
            elif pos.side == "SHORT" and last_price <= pos.reversion_tp1_price and pos.reversion_tp1_price > 0:
                return "회귀TP1(50%회복)", SPLIT_EXIT_RATIOS[0]

        # 2차 회귀 익절: 이탈 거리 80% 회복
        if pos.exit_phase == 1 and unrealized > 0:
            if pos.side == "LONG" and last_price >= pos.reversion_tp2_price and pos.reversion_tp2_price > 0:
                return "회귀TP2(80%회복)", SPLIT_EXIT_RATIOS[1]
            elif pos.side == "SHORT" and last_price <= pos.reversion_tp2_price and pos.reversion_tp2_price > 0:
                return "회귀TP2(80%회복)", SPLIT_EXIT_RATIOS[1]

        # 회귀 실패: 이탈 극값을 다시 넘어가면 전량 손절
        if pos.side == "LONG" and last_price < pos.reversion_deviation_peak_price:
            return "회귀실패(재이탈)", 1.0
        elif pos.side == "SHORT" and last_price > pos.reversion_deviation_peak_price:
            return "회귀실패(재이탈)", 1.0
        # 나머지는 일반 청산 로직으로 폴스루

    # === [v8] Q-Pulse Armed 회귀 전용 익절 ===
    if pos.strategy == "QPULSE_REVERSION":
        # 1차 익절: 극값→진입 거리의 50% 회복
        if pos.exit_phase == 0 and unrealized > 0 and pos.qp_armed_tp1_price > 0:
            if pos.side == "LONG" and last_price >= pos.qp_armed_tp1_price:
                return "QP-Armed TP1(50%회복)", SPLIT_EXIT_RATIOS[0]
            elif pos.side == "SHORT" and last_price <= pos.qp_armed_tp1_price:
                return "QP-Armed TP1(50%회복)", SPLIT_EXIT_RATIOS[0]
        # 2차 익절: 80% 회복
        if pos.exit_phase == 1 and unrealized > 0 and pos.qp_armed_tp2_price > 0:
            if pos.side == "LONG" and last_price >= pos.qp_armed_tp2_price:
                return "QP-Armed TP2(80%회복)", SPLIT_EXIT_RATIOS[1]
            elif pos.side == "SHORT" and last_price <= pos.qp_armed_tp2_price:
                return "QP-Armed TP2(80%회복)", SPLIT_EXIT_RATIOS[1]
        # 회귀 실패: 극값 재돌파 시 전량 손절
        if pos.side == "LONG" and last_price < pos.qp_armed_peak_price:
            return "QP-Armed 회귀실패(재이탈)", 1.0
        elif pos.side == "SHORT" and last_price > pos.qp_armed_peak_price:
            return "QP-Armed 회귀실패(재이탈)", 1.0

    # === [lab1 융합] Q-Pulse BB 역추세 전용 익절 ===
    if pos.strategy == "QPULSE_BB":
        # 1차 익절: BB 중심선 도달
        if pos.exit_phase == 0 and unrealized > 0 and pos.qpulse_bb_middle > 0:
            if pos.side == "LONG" and last_price >= pos.qpulse_bb_middle:
                return "QPulse익절(BB중심)", SPLIT_EXIT_RATIOS[0]
            elif pos.side == "SHORT" and last_price <= pos.qpulse_bb_middle:
                return "QPulse익절(BB중심)", SPLIT_EXIT_RATIOS[0]

    # 1. 조기 손절
    if pos.cycles_held <= EARLY_CUT_CYCLES and unrealized < -EARLY_CUT_MIN_LOSS_PCT:
        if has_stats:
            early_cut_level = -exit_profile["mae_p75"] * EARLY_CUT_MAE_MULT
        else:
            early_cut_level = -(atr15 / last_price * 100 * 1.2)
        early_cut_level = min(early_cut_level, -EARLY_CUT_MIN_LOSS_PCT)
        if unrealized <= early_cut_level:
            return "조기손절", 1.0

    # 2. 트레일링 스탑
    pos.trailing.update(last_price)
    if pos.trailing.is_hit(last_price):
        return "트레일링스탑", 1.0

    # 3. 시간 정지
    if has_stats and hold_sec > TIME_STOP_MIN_HOLD_SEC:
        median_hold = max(exit_profile["mfe_time_median"], 300.0)
        if hold_sec > median_hold * TIME_STOP_BREAKEVEN_MULT and abs(unrealized) < 0.08:
            pos.trailing.force_breakeven()
            if pos.trailing.is_hit(last_price):
                return "시간정지(본전)", 1.0
        if hold_sec > median_hold * TIME_STOP_STALE_MULT:
            if unrealized > 0.2:
                pos.trailing.force_tighten(0.80)
                if pos.trailing.is_hit(last_price):
                    return "시간정지(보호)", 1.0
            elif unrealized < -0.15:
                return "시간정지(손절)", 1.0

    # 4. 1차 부분 익절
    if pos.exit_phase == 0 and unrealized > 0 and pos.strategy not in ("REVERSION", "FUSION", "QPULSE_REVERSION"):
        if has_stats:
            tp1_target = exit_profile["mfe_p40"]
        else:
            tp1_target = atr15 / last_price * 100 * 0.8
        tp1_target = max(tp1_target, TOTAL_COST_PCT * 3, 0.30)
        if unrealized >= tp1_target:
            if not is_trend_extending:
                return "1차익절(p40)", SPLIT_EXIT_RATIOS[0]

    # 5. 2차 부분 익절
    if pos.exit_phase == 1 and unrealized > 0 and pos.strategy not in ("REVERSION", "FUSION", "QPULSE_REVERSION"):
        should_tp2 = False
        if has_stats:
            tp2_target = exit_profile["mfe_p65"]
            if unrealized >= tp2_target:
                should_tp2 = True
        else:
            tp2_target = atr15 / last_price * 100 * 1.5
            if unrealized >= tp2_target:
                should_tp2 = True

        if not should_tp2 and unrealized > 0.3:
            if pos.side == "LONG" and accel < -0.015 and speed < 0:
                should_tp2 = True
            elif pos.side == "SHORT" and accel > 0.015 and speed > 0:
                should_tp2 = True

        if not should_tp2 and has_stats and unrealized > 0.3:
            if exit_profile["prob_exceed_current"] < 0.30:
                should_tp2 = True

        if should_tp2:
            if is_trend_extending:
                return "2차익절(감속)", SPLIT_EXIT_RATIOS[1] * 0.5
            else:
                return "2차익절(p65)", SPLIT_EXIT_RATIOS[1]

    # 6. 추세 반전
    if pos.strategy in ["TREND", "REVERSION", "QPULSE_REVERSION"]:
        rev_scan = stat_analyzer.scan_opportunity(
            p_up=p_up, delta_p=delta_p, accel=accel,
            sync_score=sync_score, now=now,
        )
        if pos.side == "LONG" and rev_scan["short_edge"] > TOTAL_COST_PCT * 2:
            return "추세반전(통계)", 1.0
        elif pos.side == "SHORT" and rev_scan["long_edge"] > TOTAL_COST_PCT * 2:
            return "추세반전(통계)", 1.0
        elif rev_scan["ess"] < 8:
            if pos.side == "LONG" and p_up < 0.35:
                return "추세반전(p_up)", 1.0
            elif pos.side == "SHORT" and p_up > 0.65:
                return "추세반전(p_up)", 1.0

    # 7. 횡보 전략 중심 복귀
    if pos.strategy == "RANGE" and range_signal["action"] == "EXIT_BOTH":
        return range_signal["reason"], 1.0

    # 8. Δp 급반전
    dp_emergency = 0.10
    if has_stats and exit_profile["n"] >= 15:
        dp_emergency = max(0.06, exit_profile["mae_p75"] / 100.0 * 2)
    elif has_stats and exit_profile["n"] >= 5:
        dp_emergency = max(0.07, exit_profile["mae_p50"] / 100.0 * 2.5)

    if pos.side == "LONG" and delta_p < -dp_emergency:
        return "Δp급락", 1.0
    elif pos.side == "SHORT" and delta_p > dp_emergency:
        return "Δp급등", 1.0

    # 9. 수익 반납
    if pos.best_unrealized > 0.8 and unrealized < pos.best_unrealized * 0.25:
        return "수익반납", 1.0

    return None, 0.0


# ============================================================
# 출력
# ============================================================
def print_header():
    print("=" * 70)
    print(f"[{fmt_time()}] 🚀 Bybit 고급 매매봇 v7 시작 — 회귀 전략 통합")
    print(f"심볼: {SYMBOL} | 초기잔고: ${START_BALANCE:,.0f}")
    print(f"리스크: 최대DD {MAX_DRAWDOWN_PCT}% | 연속손실 {MAX_CONSECUTIVE_LOSSES}회")
    print(f"")
    print(f"🔴 v7 핵심: 재입학 회귀 전략")
    print(f"   벗어날 때 X → 돌아올 때 진입 (승률 {REVERSION_TARGET_WINRATE:.0f}%+ 목표)")
    print(f"   이탈 감지: p_up ±{REVERSION_DEVIATION_THRESHOLD} 이상")
    print(f"   회귀 확인: delta_p 반전 {REVERSION_CONFIRM_CYCLES}사이클 연속")
    print(f"   손절: 이탈 극값 +{REVERSION_SL_BUFFER_PCT}% (짧고 명확)")
    print(f"   익절: 이탈거리 {REVERSION_TP1_RATIO:.0%}/{REVERSION_TP2_RATIO:.0%} 회복")
    print(f"   승률 {REVERSION_PAUSE_WINRATE:.0f}% 미만 → 회귀 전략 일시 중단")
    print(f"")
    print(f"🔵 기존 유지: 커널회귀 + 모멘텀 + 회귀필터 적용")
    print(f"   분할진입: {SPLIT_ENTRY_RATIOS} | 분할청산: {SPLIT_EXIT_RATIOS}")
    print(f"   트레일링/조기손절/시간정지/물타기방지 모두 유지")
    print("=" * 70)


def print_status(mode, trend_word, conf_pct, phase, entry_info, tp_info, sl_price,
                 sync_score=0, aggression="", reversion_status="", debug=None):
    sync_str = f"동조={sync_score:+d}" if sync_score != 0 else ""
    rev_str = f" | {reversion_status}" if reversion_status else ""
    print(f"[{fmt_time()}] {phase} | {mode} | {trend_word}({conf_pct}%) {sync_str} {aggression}{rev_str}")
    print(f"[{fmt_time()}] 진입: {entry_info}")
    if tp_info != "-":
        print(f"[{fmt_time()}] 익절: {tp_info}")
    print(f"[{fmt_time()}] 손절: {fmt_price(sl_price)}")
    if debug and SHOW_DEBUG:
        print(f"[{fmt_time()}] {debug}")
    print("-" * 50)


def print_performance(perf: Performance, stat: StatAnalyzer,
                      feedback: PerformanceFeedback, reversion: MeanReversionTracker,
                      qpulse: "QpulseSetupTracker" = None,
                      qp_armed: "QPulseArmedReversionTracker" = None):
    dd = perf.current_drawdown_pct()
    pf = perf.profit_factor()
    sharpe = perf.sharpe_ratio()
    _, agg_label = feedback.get_aggression()

    n_completed = sum(1 for r in stat.records if r.filled_15m)
    if n_completed < 30:
        phase = "수집중"
    elif n_completed >= 100:
        phase = "통계(풀)"
    else:
        phase = "통계"

    decay_status = "정상" if stat.decay_lambda == stat.base_decay_lambda else f"가속({stat.decay_lambda:.4f})"
    rev_wr = reversion.get_winrate()
    rev_wr_str = f"{rev_wr:.0f}%" if rev_wr is not None else "N/A"

    print(f"[{fmt_time()}] [성과] "
          f"수익 {perf.total_return_pct():+.2f}% | 잔고 ${perf.balance:,.2f} | "
          f"승률 {perf.winrate():.1f}% | PF {pf:.2f} | 샤프 {sharpe:.2f}")
    print(f"[{fmt_time()}] [상태] "
          f"DD {dd:.1f}% | 연손 {perf.consecutive_losses} | "
          f"{phase}(샘플{stat.total_records}) | 공격성:{agg_label} | 감쇠:{decay_status}")
    print(f"[{fmt_time()}] [회귀] "
          f"승률={rev_wr_str}({reversion.reversion_total}건) | "
          f"회귀PnL=${perf.reversion_pnl:+.2f} | "
          f"{'중단' if reversion.is_paused else '활성'}")
    # [lab1 융합] Q-Pulse BB 성과
    if qpulse is not None:
        qp_wr = qpulse.get_winrate()
        qp_wr_str = f"{qp_wr:.0f}%" if qp_wr is not None else "N/A"
        print(f"[{fmt_time()}] [QPulse-BB] "
              f"승률={qp_wr_str}({qpulse.total_signals}건) | "
              f"PnL=${perf.qpulse_pnl:+.2f} | "
              f"상태={qpulse.get_status()}")
    # [v8] Q-Pulse Armed 회귀 성과
    if qp_armed is not None:
        qa_wr = qp_armed.get_winrate()
        qa_wr_str = f"{qa_wr:.0f}%" if qa_wr is not None else "N/A"
        print(f"[{fmt_time()}] [QP-Armed] "
              f"승률={qa_wr_str}({qp_armed.total}건) | "
              f"PnL=${perf.qp_armed_rev_pnl:+.2f} | "
              f"상태={qp_armed.get_status()}")
    print("=" * 70)


# ============================================================
# 텔레그램 알림
# ============================================================
class TelegramBot:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self._q: _queue.Queue = _queue.Queue()
        t = threading.Thread(target=self._worker, daemon=True)
        t.start()

    def send(self, text: str):
        self._q.put(f"[Xian] {text}")

    def _worker(self):
        while True:
            text = self._q.get()
            try:
                url = f"https://api.telegram.org/bot{self.token}/sendMessage"
                requests.post(url, data={"chat_id": self.chat_id, "text": text}, timeout=10)
            except Exception as e:
                print(f"텔레그램 전송 실패: {e}")
            finally:
                self._q.task_done()


# ============================================================
# 메인
# ============================================================
def main():
    print_header()

    cal = Calibrator()
    perf = Performance()
    stat = StatAnalyzer()
    momentum = MomentumTracker()
    feedback = PerformanceFeedback()
    reversion = MeanReversionTracker()
    qpulse_tracker = QpulseSetupTracker()         # [lab1 융합]
    qp_armed_tracker = QPulseArmedReversionTracker()  # [v8]
    tg = TelegramBot(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)

    positions: List[SplitPosition] = []   # 동시 추적 포지션 목록
    signal_counter: int = 0               # 누적 시그널 번호
    last_alerted_side: Optional[str] = None  # 마지막으로 알람 보낸 방향
    prev_tf_data: Optional[dict] = None

    last_price_15m: Optional[float] = None
    retry_count = 0
    tf_cache = {}
    last_fetch_time = {}
    last_perf_print = datetime.now(timezone.utc)
    atr_history: deque = deque(maxlen=50)

    last_exit_time: Optional[datetime] = None
    last_exit_side: Optional[str] = None

    in_cooldown = False
    cooldown_until: Optional[datetime] = None

    while True:
        try:
            now = datetime.now(timezone.utc)

            # 쿨다운 체크
            if in_cooldown:
                if now >= cooldown_until:
                    in_cooldown = False
                    perf.consecutive_losses = 0
                    if not SIGNAL_ONLY:
                        print(f"[{fmt_time()}] 휴식 종료, 매매 재개")
                else:
                    remaining = (cooldown_until - now).total_seconds() / 60
                    if not SIGNAL_ONLY:
                        print(f"[{fmt_time()}] 휴식 중 ({remaining:.0f}분 남음)")
                    time.sleep(REFRESH_SEC)
                    continue

            if perf.consecutive_losses >= MAX_CONSECUTIVE_LOSSES and not in_cooldown:
                in_cooldown = True
                cooldown_until = now + timedelta(minutes=COOLDOWN_MINUTES)
                if not SIGNAL_ONLY:
                    print(f"[{fmt_time()}] 연속 {perf.consecutive_losses}회 손실 -> {COOLDOWN_MINUTES}분 휴식")
                time.sleep(REFRESH_SEC)
                continue

            # === 데이터 수집 ===
            tf = {}
            for name, interval in TIMEFRAMES.items():
                cache_valid = (name in tf_cache and name in last_fetch_time and
                               (now - last_fetch_time[name]).total_seconds() < CACHE_SECONDS[name])
                if cache_valid:
                    tf[name] = tf_cache[name]
                else:
                    df = fetch_klines(SYMBOL, interval, CANDLES_LIMIT, CATEGORY)
                    tf[name] = classify_trend(df)
                    tf_cache[name] = tf[name]
                    last_fetch_time[name] = now
                    time.sleep(0.2)

            base = combine_timeframes(tf)
            p_up_base = base["p_up_base"]
            p_up = cal.calibrate(p_up_base)
            is_ranging = base["is_ranging"]
            cal.update_on_15m_close(tf["15m"]["time"], tf["15m"]["close"], p_up_base)

            last_price = fetch_last_price(SYMBOL, CATEGORY)
            atr15 = tf["15m"]["atr14"]
            atr_history.append(atr15)
            avg_atr = float(np.mean(atr_history))

            # === 모멘텀 ===
            delta_p, speed, accel = momentum.update(p_up)

            # === 멀티TF 동조 ===
            sync_score = calc_sync_score(tf, prev_tf_data)
            prev_tf_data = {k: {"score": v["score"]} for k, v in tf.items()}

            # === 통계 레코드 기록 ===
            rec = StatRecord(
                timestamp=now, p_up=p_up, delta_p=delta_p,
                delta_p_speed=speed, delta_p_accel=accel,
                price=last_price, sync_score=sync_score,
            )
            stat.add_record(rec)
            stat.fill_future_prices(last_price, now)
            stat.update_mfe_mae_realtime(last_price)

            # === 상태 변수 ===
            trend_word = "상승" if p_up >= 0.5 else "하락"
            conf_pct = int(round(p_up * 100))
            mode = "횡보" if is_ranging else "추세"
            _, aggression_label = feedback.get_aggression()

            quick_scan = stat.scan_opportunity(p_up, delta_p, accel, sync_score, now)
            ess_now = quick_scan["ess"]
            if stat.total_records < 30:
                phase_label = "수집중"
            elif ess_now >= 15:
                phase_label = f"통계(E{ess_now:.0f})"
            else:
                phase_label = f"모멘텀(E{ess_now:.0f})"

            range_signal = check_range_signal(tf, RANGE_RSI_OVERSOLD, RANGE_RSI_OVERBOUGHT) if is_ranging else {"action": None}

            # [lab1 융합] Q-Pulse BB 신호 계산 (15m 기준)
            tf15 = tf["15m"]
            qpulse_signal = qpulse_tracker.update(
                ema19=tf15["ema19"], ema40=tf15["ema40"],
                ema19_prev=tf15["ema19_prev"], ema40_prev=tf15["ema40_prev"],
                qpulse=tf15["qpulse"], close=tf15["close"],
                bb_upper=tf15["bb_upper"], bb_lower=tf15["bb_lower"],
                bb_middle=tf15["bb_middle"],
            )

            # [v8] Q-Pulse 멀티TF 스코어 + Armed 회귀 신호
            qpulse_mtf_score = calc_qpulse_multitf_score(tf)
            qp_armed_signal = qp_armed_tracker.update(
                qpulse_score=qpulse_mtf_score,
                price=last_price,
                delta_p=delta_p,
                now=now,
            )

            if last_price_15m is None:
                delta_15m = 0.0
            else:
                delta_15m = tf["15m"]["close"] - last_price_15m
            last_price_15m = tf["15m"]["close"]

            # 회귀 트래커 상태
            reversion_status = reversion.get_status()

            # ============================================
            # 진입 판단 (포지션 유무 관계없이 항상 체크)
            # ============================================
            decision = decide_entry(
                p_up=p_up, delta_p=delta_p, accel=accel,
                sync_score=sync_score, is_ranging=is_ranging,
                range_signal=range_signal, stat_analyzer=stat,
                perf=perf, feedback=feedback,
                reversion_tracker=reversion,
                qpulse_tracker=qpulse_tracker,
                qpulse_signal=qpulse_signal,
                qp_armed_tracker=qp_armed_tracker,
                qp_armed_signal=qp_armed_signal,
                now=now,
                last_price=last_price, atr15=atr15, avg_atr=avg_atr, tf=tf,
                last_exit_time=last_exit_time, last_exit_side=last_exit_side,
            )

            if decision.should_enter:
                signal_counter += 1
                sid = signal_counter

                if len(positions) < MAX_CONCURRENT_SIGNALS:
                    entry_px = apply_slippage(last_price, decision.side, is_entry=True)
                    available = perf.balance * decision.position_ratio
                    notional = available * LEVERAGE
                    qty = notional / entry_px
                    fees = fee(notional)
                    perf.balance -= fees

                    new_pos = SplitPosition(side=decision.side, strategy=decision.strategy,
                                            signal_id=sid)
                    new_pos.add_entry(entry_px, qty, now, fees)
                    new_pos.initial_stop = decision.stop_price
                    if decision.is_reversion:
                        new_pos.reversion_deviation_peak_price = decision.reversion_deviation_peak_price
                        new_pos.reversion_tp1_price = decision.reversion_tp1_price
                        new_pos.reversion_tp2_price = decision.reversion_tp2_price
                    if decision.is_qpulse_bb:
                        new_pos.qpulse_bb_middle = decision.qpulse_bb_middle
                    if decision.is_qp_armed_rev:
                        new_pos.qp_armed_peak_price = decision.qp_armed_peak_price
                        new_pos.qp_armed_tp1_price = decision.qp_armed_tp1_price
                        new_pos.qp_armed_tp2_price = decision.qp_armed_tp2_price
                    new_pos.trailing.init(entry_px, decision.side, decision.stop_price, atr15)
                    positions.append(new_pos)

                    print(f"\n{'='*60}")
                    print(f"  [진입 #{sid}]  [{fmt_time()}]  ({len(positions)}/{MAX_CONCURRENT_SIGNALS})")
                    print(f"  방향   : {decision.side}  |  전략: {decision.strategy}")
                    print(f"  가격   : {fmt_price(entry_px)}  |  손절: {fmt_price(decision.stop_price)}")
                    if decision.is_reversion:
                        print(f"  TP1    : {fmt_price(decision.reversion_tp1_price)}"
                              f"  TP2: {fmt_price(decision.reversion_tp2_price)}")
                    elif decision.is_qpulse_bb:
                        print(f"  TP1    : {fmt_price(decision.qpulse_bb_middle)}  (BB 중심선)")
                    elif decision.is_qp_armed_rev:
                        print(f"  극값   : {fmt_price(decision.qp_armed_peak_price)}"
                              f"  TP1: {fmt_price(decision.qp_armed_tp1_price)}"
                              f"  TP2: {fmt_price(decision.qp_armed_tp2_price)}")
                    print(f"  신뢰도 : {decision.confidence_tier}  |  이유: {decision.reason}")
                    print(f"{'='*60}\n")

                    _tp_line = ""
                    if decision.is_reversion:
                        _tp_line = f"\nTP1: {fmt_price(decision.reversion_tp1_price)}  TP2: {fmt_price(decision.reversion_tp2_price)}"
                    elif decision.is_qpulse_bb:
                        _tp_line = f"\nTP1: {fmt_price(decision.qpulse_bb_middle)} (BB중심)"
                    elif decision.is_qp_armed_rev:
                        _tp_line = (f"\n극값: {fmt_price(decision.qp_armed_peak_price)}"
                                    f"  TP1: {fmt_price(decision.qp_armed_tp1_price)}"
                                    f"  TP2: {fmt_price(decision.qp_armed_tp2_price)}")
                    if decision.side != last_alerted_side:
                        tg.send(
                            f"[진입 #{sid}] {decision.side} | {decision.strategy}\n"
                            f"가격: {fmt_price(entry_px)}  손절: {fmt_price(decision.stop_price)}"
                            f"{_tp_line}\n"
                            f"신뢰도: {decision.confidence_tier}  포지션: {len(positions)}/{MAX_CONCURRENT_SIGNALS}"
                        )
                        last_alerted_side = decision.side
                else:
                    print(f"[{fmt_time()}] [시그널 #{sid} 스킵] 최대 포지션({MAX_CONCURRENT_SIGNALS}) 도달 "
                          f"| {decision.side} @ {fmt_price(last_price)} | {decision.strategy}")
            else:
                if not SIGNAL_ONLY and not positions:
                    if (now - last_perf_print).total_seconds() >= 60:
                        print_performance(perf, stat, feedback, reversion,
                                          qpulse=qpulse_tracker, qp_armed=qp_armed_tracker)
                        last_perf_print = now

            # ============================================
            # 포지션 관리 (각 포지션 독립 청산)
            # ============================================
            for pos in positions[:]:
                pos.tick(last_price)

                # 분할 추가 진입 (회귀/융합 전략 제외)
                if pos.strategy not in ("REVERSION", "FUSION", "QPULSE_REVERSION") and pos.should_add_entry(last_price, delta_p):
                    phase_idx = pos.entry_phase
                    if phase_idx < len(SPLIT_ENTRY_RATIOS):
                        add_ratio = SPLIT_ENTRY_RATIOS[phase_idx]
                        aggression_mult, _ = feedback.get_aggression()
                        add_available = perf.balance * kelly_criterion(
                            perf.winrate(), perf.avg_win_loss_ratio(), BASE_POSITION_RATIO
                        ) * aggression_mult * add_ratio
                        add_px = apply_slippage(last_price, pos.side, is_entry=True)
                        add_notional = add_available * LEVERAGE
                        add_qty = add_notional / add_px
                        add_fees = fee(add_notional)
                        perf.balance -= add_fees
                        pos.add_entry(add_px, add_qty, now, add_fees)
                        if not SIGNAL_ONLY:
                            print(f"[#{pos.signal_id}] {pos.entry_phase}차 추가진입 @ {fmt_price(add_px)}")

                # 청산 판단
                exit_reason, exit_ratio = decide_exit(
                    pos=pos, p_up=p_up, delta_p=delta_p,
                    accel=accel, speed=speed, sync_score=sync_score,
                    last_price=last_price,
                    is_ranging=is_ranging, range_signal=range_signal,
                    stat_analyzer=stat, now=now, atr15=atr15,
                )

                unrealized = pos.unrealized_pnl_pct(last_price)
                trail_stop = pos.trailing.current_stop

                if exit_reason is None:
                    if not SIGNAL_ONLY:
                        print(f"[#{pos.signal_id}] {pos.side}({pos.strategy}) "
                              f"@ {fmt_price(pos.avg_entry_price)} | "
                              f"미실현 {unrealized:+.2f}% | 손절 {fmt_price(trail_stop)}")
                    continue

                # 청산 실행
                exit_qty = pos.remaining_qty * exit_ratio
                exit_px = apply_slippage(last_price, pos.side, is_entry=False)
                exit_fees = fee(exit_qty * exit_px)
                perf.balance -= exit_fees

                pnl = pos.partial_exit(exit_qty, exit_px, exit_fees)
                perf.balance += pnl
                perf.realized_pnl += pnl
                perf.update_peak()

                pnl_pct = pnl / START_BALANCE * 100.0
                feedback.add_trade(pnl_pct)

                is_full_exit = (pos.remaining_qty < 0.0001) or exit_ratio >= 0.99

                if is_full_exit:
                    last_exit_time = now
                    last_exit_side = pos.side

                    perf.trades += 1
                    if pnl < 0:
                        perf.consecutive_losses += 1
                        perf.stoplosses += 1
                    else:
                        perf.consecutive_losses = 0

                    if pos.side == "LONG":
                        perf.long_pnl += pnl
                        perf.long_trades += 1
                    else:
                        perf.short_pnl += pnl
                        perf.short_trades += 1

                    if pos.strategy == "TREND":
                        perf.trend_pnl += pnl
                        perf.trend_trades += 1
                    elif pos.strategy == "RANGE":
                        perf.range_pnl += pnl
                        perf.range_trades += 1
                    elif pos.strategy in ("REVERSION", "FUSION"):
                        perf.reversion_pnl += pnl
                        perf.reversion_trades += 1
                        reversion.record_trade_result(pnl > 0)
                        if pos.strategy == "FUSION":
                            qpulse_tracker.record_result(pnl > 0)
                    elif pos.strategy == "QPULSE_BB":
                        perf.qpulse_pnl += pnl
                        perf.qpulse_trades += 1
                        qpulse_tracker.record_result(pnl > 0)
                    elif pos.strategy == "QPULSE_REVERSION":
                        perf.qp_armed_rev_pnl += pnl
                        perf.qp_armed_rev_trades += 1
                        qp_armed_tracker.record_trade_result(pnl > 0)

                    entry_hour = pos.entry_start_time.hour if pos.entry_start_time else now.hour
                    perf.hourly_pnl[entry_hour] = perf.hourly_pnl.get(entry_hour, 0) + pnl
                    perf.hourly_trades[entry_hour] = perf.hourly_trades.get(entry_hour, 0) + 1
                    perf.trade_history.append(Trade(
                        entry_time=pos.entry_start_time or now,
                        exit_time=now, side=pos.side, strategy=pos.strategy,
                        entry_price=pos.avg_entry_price, exit_price=exit_px,
                        pnl=pnl, exit_reason=exit_reason, hour=entry_hour,
                    ))

                    predicted_up = p_up >= 0.5
                    actual_up = exit_px > pos.avg_entry_price
                    stat.add_prediction_result(predicted_up == actual_up)

                    pnl_emoji = "✅" if pnl >= 0 else "❌"
                    print(f"\n{'='*60}")
                    print(f"  {pnl_emoji} [청산 #{pos.signal_id}]  [{fmt_time()}]")
                    print(f"  방향   : {pos.side}  |  전략: {pos.strategy}")
                    print(f"  청산가 : {fmt_price(exit_px)}  (진입: {fmt_price(pos.avg_entry_price)})")
                    print(f"  사유   : {exit_reason}")
                    print(f"  손익   : ${pnl:+.2f}  ({pnl_pct:+.3f}%)")
                    print(f"  잔고   : ${perf.balance:.2f}  |  총 거래: {perf.trades}회")
                    print(f"{'='*60}\n")
                    tg.send(
                        f"{pnl_emoji} [청산 #{pos.signal_id}] {pos.side} | {pos.strategy}\n"
                        f"청산가: {fmt_price(exit_px)}  (진입: {fmt_price(pos.avg_entry_price)})\n"
                        f"사유: {exit_reason}\n"
                        f"손익: ${pnl:+.2f} ({pnl_pct:+.3f}%)  잔고: ${perf.balance:.2f}"
                    )

                    positions.remove(pos)
                    last_alerted_side = None  # 청산 후 방향 리셋 → 다음 동일 방향도 알람
                    last_perf_print = now
                else:
                    print(f"[{fmt_time()}] [#{pos.signal_id} 부분청산] {exit_ratio:.0%} "
                          f"@ {fmt_price(exit_px)} | PnL: ${pnl:+.2f} "
                          f"| 잔여 {pos.remaining_qty:.6f}")
                    tg.send(
                        f"[#{pos.signal_id} 부분청산] {exit_ratio:.0%} {pos.side}\n"
                        f"@ {fmt_price(exit_px)}  PnL: ${pnl:+.2f}\n"
                        f"사유: {exit_reason}  잔여: {pos.remaining_qty:.6f}"
                    )

            retry_count = 0
            time.sleep(REFRESH_SEC)

        except Exception as e:
            retry_count += 1
            if "Rate Limit" in str(e) or "10006" in str(e):
                wait_time = 60
                print(f"[{fmt_time()}] ⚠️ API 제한 ({retry_count}/{MAX_RETRIES}). {wait_time}초 대기...")
            else:
                wait_time = REFRESH_SEC
                print(f"[{fmt_time()}] 오류 ({retry_count}/{MAX_RETRIES}): {type(e).__name__}: {e}")
            if SHOW_DEBUG:
                print(traceback.format_exc())
            if retry_count >= MAX_RETRIES:
                print(f"[{fmt_time()}] ⛔ {MAX_RETRIES}회 연속 실패. 종료.")
                break
            time.sleep(wait_time)


if __name__ == "__main__":
    main()
