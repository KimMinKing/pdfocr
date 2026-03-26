#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
텔레그램 커맨더 — 발신 + 수신(폴링) + 인라인 버튼 메뉴

버튼:
  [📊 상태]  → 업타임 / 포지션 현황
  [💰 수익]  → 세션 PnL / 거래수 / 승률
  [📋 최근거래] → 가장 최근 청산 상세
"""

import queue
import threading
import time
import requests
from datetime import datetime, timezone


class TelegramCommander:
    """TelegramBot 대체 — 기존 send() API 그대로 유지하면서 버튼 수신 추가"""

    def __init__(self, token: str, chat_id: str, gs, states, max_pos: int):
        """
        gs      : GlobalState (start_time, session_trades, count() 포함)
        states  : List[BotSymbolState]
        max_pos : MAX_GLOBAL_POS 값
        """
        self.token    = token
        self.chat_id  = str(chat_id)
        self.gs       = gs
        self.states   = states
        self.max_pos  = max_pos
        self._offset  = 0
        self._q: queue.Queue = queue.Queue()

        threading.Thread(target=self._send_worker, daemon=True).start()
        threading.Thread(target=self._poll_loop,   daemon=True).start()

    # ================================================================
    # Public API  (기존 TelegramBot.send()와 동일하게 사용)
    # ================================================================

    def send(self, text: str):
        """봇 이벤트 메시지 전송"""
        self._enqueue("sendMessage", {
            "chat_id": self.chat_id,
            "text": f"[Xian] {text}",
        })

    # ================================================================
    # 내부 — 전송 큐
    # ================================================================

    def _enqueue(self, method: str, data: dict):
        self._q.put((method, data))

    def _send_worker(self):
        while True:
            method, data = self._q.get()
            try:
                requests.post(
                    f"https://api.telegram.org/bot{self.token}/{method}",
                    json=data,
                    timeout=10,
                )
            except Exception as e:
                print(f"텔레그램 전송 실패: {e}")
            finally:
                self._q.task_done()

    # ================================================================
    # 내부 — 폴링
    # ================================================================

    def _poll_loop(self):
        while True:
            try:
                r = requests.get(
                    f"https://api.telegram.org/bot{self.token}/getUpdates",
                    params={"offset": self._offset, "timeout": 15},
                    timeout=20,
                ).json()
                for upd in r.get("result", []):
                    self._offset = upd["update_id"] + 1
                    self._handle(upd)
            except Exception as e:
                print(f"텔레그램 폴링 오류: {e}")
                time.sleep(5)

    def _handle(self, upd: dict):
        # 버튼 클릭
        if "callback_query" in upd:
            cq      = upd["callback_query"]
            chat_id = str(cq["message"]["chat"]["id"])
            data    = cq.get("data", "")
            self._enqueue("answerCallbackQuery", {"callback_query_id": cq["id"]})
            if data == "status":
                self._reply_status(chat_id)
            elif data == "pnl":
                self._reply_pnl(chat_id)
            elif data == "last":
                self._reply_last(chat_id)
            # 버튼 응답 후 메뉴 다시 표시
            self._send_menu(chat_id)
            return

        # 일반 메시지 수신 → 메뉴 표시
        if "message" in upd:
            chat_id = str(upd["message"]["chat"]["id"])
            self._send_menu(chat_id)

    # ================================================================
    # 메뉴
    # ================================================================

    def _send_menu(self, chat_id: str):
        self._enqueue("sendMessage", {
            "chat_id": chat_id,
            "text": "명령을 선택하세요:",
            "reply_markup": {
                "inline_keyboard": [[
                    {"text": "📊 상태",      "callback_data": "status"},
                    {"text": "💰 수익",      "callback_data": "pnl"},
                    {"text": "📋 최근거래",  "callback_data": "last"},
                ]]
            },
        })

    # ================================================================
    # 응답 — 상태
    # ================================================================

    def _reply_status(self, chat_id: str):
        now    = datetime.now(timezone.utc)
        uptime = now - self.gs.start_time
        h, rem = divmod(int(uptime.total_seconds()), 3600)
        m      = rem // 60

        lines = [
            f"🤖 봇 상태",
            f"업타임: {h}시간 {m}분",
            f"포지션: {self.gs.count()}/{self.max_pos}개",
            "",
        ]
        for sym in self.states:
            if sym.positions:
                pos      = sym.positions[0]
                hold_min = (
                    int((now - pos.entry_start_time).total_seconds() / 60)
                    if pos.entry_start_time else 0
                )
                lines.append(f"  {sym.symbol}: {pos.side}  {hold_min}분 보유")
            else:
                lines.append(f"  {sym.symbol}: 대기 중")

        self._enqueue("sendMessage", {"chat_id": chat_id, "text": "\n".join(lines)})

    # ================================================================
    # 응답 — 수익
    # ================================================================

    def _reply_pnl(self, chat_id: str):
        trades      = self.gs.session_trades
        total_pnl   = sum(t["pnl"] for t in trades)
        total_count = len(trades)
        wins        = sum(1 for t in trades if t["pnl"] > 0)
        wr          = wins / total_count * 100 if total_count else 0.0
        since       = self.gs.start_time.strftime("%m/%d %H:%M")

        lines = [
            f"💰 세션 수익  ({since} ~)",
            "",
            f"총 PnL : ${total_pnl:+.2f}",
            f"거래수 : {total_count}건  (승률 {wr:.0f}%)",
        ]
        if trades:
            by_sym: dict = {}
            for t in trades:
                by_sym[t["symbol"]] = by_sym.get(t["symbol"], 0.0) + t["pnl"]
            lines.append("")
            for sym, pnl in sorted(by_sym.items()):
                cnt = sum(1 for t in trades if t["symbol"] == sym)
                lines.append(f"  {sym}: ${pnl:+.2f}  ({cnt}건)")

        self._enqueue("sendMessage", {"chat_id": chat_id, "text": "\n".join(lines)})

    # ================================================================
    # 응답 — 최근 거래
    # ================================================================

    def _reply_last(self, chat_id: str):
        trades = self.gs.session_trades
        if not trades:
            self._enqueue("sendMessage", {
                "chat_id": chat_id,
                "text": "📋 세션 시작 후 거래 없음",
            })
            return

        t     = trades[-1]
        emoji = "✅" if t["pnl"] >= 0 else "❌"
        text  = (
            f"📋 최근 거래\n\n"
            f"{emoji} {t['symbol']}  {t['side']}\n"
            f"진입: {t['entry_price']:,.4g}  →  청산: {t['exit_price']:,.4g}\n"
            f"PnL: ${t['pnl']:+.2f}\n"
            f"사유: {t['reason']}\n"
            f"시간: {t['time'].strftime('%m/%d %H:%M UTC')}"
        )
        self._enqueue("sendMessage", {"chat_id": chat_id, "text": text})
