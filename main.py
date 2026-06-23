import asyncio
import json
import logging
import math
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Optional, Set

import uvicorn
from binance.client import Client
from binance.exceptions import BinanceAPIException
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

import scanner

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CREDENTIALS_FILE = Path("credentials.json")
CONFIG_FILE = Path("config.json")
MATCHES_FILE = Path("matches.json")

# ── Global state ──────────────────────────────────────────────────────────────

binance_client: Optional[Client] = None
trailing_stop_on: bool = False
global_settings: dict = {"default_sl": 37.0, "gap": 25.0, "default_pnl": 5.0}

# Per-symbol parameter overrides  {symbol: {default_sl, gap, default_pnl}}
position_overrides: Dict[str, dict] = {}

# Runtime tracking per symbol
# {symbol: {
#     gap_triggered: bool,
#     peak_roe: float,
#     current_sl_pnl: float,
#     sl_order_id: str|None,
# }}
position_states: Dict[str, dict] = {}

connected_clients: Set[WebSocket] = set()

# Event to wake up the scanner loop immediately
trigger_scan_event = asyncio.Event()

# ── Credentials helpers ───────────────────────────────────────────────────────

def load_saved_credentials() -> dict:
    if CREDENTIALS_FILE.exists():
        try:
            return json.loads(CREDENTIALS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"api_key": "", "api_secret": ""}

def save_credentials(api_key: str, api_secret: str):
    CREDENTIALS_FILE.write_text(
        json.dumps({"api_key": api_key, "api_secret": api_secret}, ensure_ascii=False),
        encoding="utf-8",
    )

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_symbol_settings(symbol: str) -> dict:
    return position_overrides.get(symbol, global_settings)

def roe_to_price(entry: float, leverage: int, pnl_pct: float, side: str) -> float:
    ratio = pnl_pct / 100.0 / leverage
    return entry * (1 + ratio) if side == "LONG" else entry * (1 - ratio)

def round_price(price: float, tick: float) -> float:
    if tick <= 0:
        return round(price, 8)
    precision = max(0, -int(math.floor(math.log10(tick))))
    return round(round(price / tick) * tick, precision)

def current_roe(pos: dict) -> float:
    try:
        entry  = float(pos.get("entryPrice", 0))
        mark   = float(pos.get("markPrice", 0))
        lev    = int(pos.get("leverage", 1))
        if entry == 0:
            return 0.0
        direction = 1 if float(pos.get("positionAmt", 0)) > 0 else -1
        return (mark - entry) / entry * lev * direction * 100
    except Exception:
        return 0.0

def current_pnl(pos: dict) -> float:
    raw = pos.get("unrealizedProfit") or pos.get("unRealizedProfit") or 0
    return float(raw)

def load_matches_file() -> list:
    if MATCHES_FILE.exists():
        try:
            return json.loads(MATCHES_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []

# ── Binance helpers ───────────────────────────────────────────────────────────

def fetch_open_positions() -> List[dict]:
    if not binance_client:
        return []
    try:
        account = binance_client.futures_account()
        positions = [p for p in account["positions"] if float(p["positionAmt"]) != 0]
        prices = {t["symbol"]: float(t["markPrice"])
                  for t in binance_client.futures_mark_price()}
        for p in positions:
            sym = p["symbol"]
            p["markPrice"] = prices.get(sym, float(p["entryPrice"]))
            p["side"] = "LONG" if float(p["positionAmt"]) > 0 else "SHORT"
            p["roe"] = current_roe(p)
            p["pnl"] = current_pnl(p)
            lev = int(p.get("leverage", 1)) or 1
            p["margin"] = abs(float(p["positionAmt"])) * float(p["entryPrice"]) / lev
            state = position_states.get(sym, {})
            p["settings"] = get_symbol_settings(sym)
            p["gap_triggered"]  = state.get("gap_triggered", False)
            p["peak_roe"]       = state.get("peak_roe", 0.0)
            p["current_sl_pnl"] = state.get("current_sl_pnl")
            p["sl_order_id"]    = state.get("sl_order_id")
        return positions
    except BinanceAPIException as e:
        log.error("fetch_open_positions: %s", e)
        return []

def cancel_sl_order(symbol: str):
    state = position_states.get(symbol, {})
    oid = state.get("sl_order_id")
    if oid and binance_client:
        try:
            binance_client.futures_cancel_order(symbol=symbol, orderId=oid)
            log.info("Cancelled SL %s for %s", oid, symbol)
        except BinanceAPIException:
            pass
    if symbol in position_states:
        position_states[symbol]["sl_order_id"] = None

def place_stop_market(symbol: str, side: str, qty: float, stop_price: float) -> Optional[str]:
    if not binance_client:
        return None
    close_side = "SELL" if side == "LONG" else "BUY"
    try:
        order = binance_client.futures_create_order(
            symbol=symbol,
            side=close_side,
            type="STOP_MARKET",
            stopPrice=f"{stop_price:.8f}".rstrip("0").rstrip("."),
            quantity=abs(qty),
            reduceOnly=True,
            timeInForce="GTE_GTC",
            workingType="MARK_PRICE",
        )
        log.info("Placed SL order %s for %s @ %.6f", order["orderId"], symbol, stop_price)
        return str(order["orderId"])
    except BinanceAPIException as e:
        log.error("place_stop_market %s: %s", symbol, e)
        return None

def close_position_market(symbol: str, side: str, qty: float):
    if not binance_client:
        return
    close_side = "SELL" if side == "LONG" else "BUY"
    try:
        order = binance_client.futures_create_order(
            symbol=symbol,
            side=close_side,
            type="MARKET",
            quantity=abs(qty),
            reduceOnly=True,
        )
        log.info("CLOSED position %s via market order %s", symbol, order["orderId"])
    except BinanceAPIException as e:
        log.error("close_position_market %s: %s", symbol, e)

# ── Background Loops ──────────────────────────────────────────────────────────

async def trailing_stop_loop():
    while True:
        await asyncio.sleep(3)
        if not trailing_stop_on or not binance_client:
            continue
        try:
            positions = fetch_open_positions()
            for pos in positions:
                symbol   = pos["symbol"]
                side     = pos["side"]
                entry    = float(pos["entryPrice"])
                qty      = float(pos["positionAmt"])
                roe      = pos["roe"]
                leverage = int(pos.get("leverage", 1))
                s        = get_symbol_settings(symbol)
                default_sl  = s["default_sl"]
                gap         = s["gap"]
                default_pnl = s["default_pnl"]

                if symbol not in position_states:
                    position_states[symbol] = {
                        "gap_triggered": False,
                        "peak_roe": 0.0,
                        "current_sl_pnl": None,
                        "sl_order_id": None,
                    }

                state = position_states[symbol]

                # ── Phase 1: Before GAP — monitor Default SL ─────────────
                if not state["gap_triggered"]:
                    if roe <= -default_sl:
                        log.info("%s ROE=%.2f%% ≤ -SL=%.2f%% → closing (default SL hit)",
                                 symbol, roe, default_sl)
                        cancel_sl_order(symbol)
                        close_position_market(symbol, side, qty)
                        position_states.pop(symbol, None)
                        await broadcast({"type": "sl_hit", "symbol": symbol,
                                         "roe": roe, "sl": -default_sl})
                        continue

                    if not state["sl_order_id"]:
                        sl_price = roe_to_price(entry, leverage, -default_sl, side)
                        oid = place_stop_market(symbol, side, qty, sl_price)
                        state["sl_order_id"] = oid
                        state["current_sl_pnl"] = -default_sl

                    if roe >= gap:
                        log.info("%s ROE=%.2f%% >= GAP=%.2f%% → activate trailing, SL=%.2f%%",
                                 symbol, roe, gap, default_pnl)
                        cancel_sl_order(symbol)
                        sl_price = roe_to_price(entry, leverage, default_pnl, side)
                        oid = place_stop_market(symbol, side, qty, sl_price)
                        state.update({
                            "gap_triggered": True,
                            "peak_roe": roe,
                            "current_sl_pnl": default_pnl,
                            "sl_order_id": oid,
                        })
                        await broadcast({"type": "gap_triggered", "symbol": symbol,
                                         "roe": roe, "sl_pnl": default_pnl})

                # ── Phase 2: Trailing — update SL as peak_roe rises ──────
                else:
                    new_peak = max(state["peak_roe"], roe)
                    new_sl_pnl = max(default_pnl, new_peak - gap)

                    if roe <= state["current_sl_pnl"]:
                        log.info("%s ROE=%.2f%% <= SL=%.2f%% → closing position (trailing SL hit)",
                                 symbol, roe, state["current_sl_pnl"])
                        cancel_sl_order(symbol)
                        close_position_market(symbol, side, qty)
                        await broadcast({"type": "sl_hit", "symbol": symbol,
                                         "roe": roe, "sl": state["current_sl_pnl"]})
                        position_states.pop(symbol, None)
                        continue

                    if new_sl_pnl > state["current_sl_pnl"] + 0.05:
                        log.info("%s peak=%.2f%% → raising SL %.2f%% → %.2f%%",
                                 symbol, new_peak, state["current_sl_pnl"], new_sl_pnl)
                        cancel_sl_order(symbol)
                        sl_price = roe_to_price(entry, leverage, new_sl_pnl, side)
                        oid = place_stop_market(symbol, side, qty, sl_price)
                        state.update({
                            "peak_roe": new_peak,
                            "current_sl_pnl": new_sl_pnl,
                            "sl_order_id": oid,
                        })
                    elif new_peak > state["peak_roe"]:
                        state["peak_roe"] = new_peak

            # Clean up positions that were closed externally
            open_symbols = {p["symbol"] for p in positions}
            for sym in list(position_states.keys()):
                if sym not in open_symbols:
                    position_states.pop(sym, None)

        except Exception as e:
            log.exception("trailing_stop_loop error: %s", e)

async def scanner_loop():
    print("Khởi động scanner background loop...")
    
    # Run scanner immediately on launch
    try:
        log.info("Đang chạy quét nến lần đầu khi khởi động...")
        await asyncio.to_thread(scanner.job)
        await broadcast({"type": "matches", "data": load_matches_file()})
    except Exception as e:
        log.error(f"Lỗi khi chạy quét nến lần đầu: {e}")
        
    while True:
        try:
            config = scanner.load_config()
            interval = config.get("interval_minutes", 15)
        except Exception:
            interval = 15
            
        try:
            # Wait for either the interval to pass or the trigger event to be set
            await asyncio.wait_for(trigger_scan_event.wait(), timeout=interval * 60)
            log.info("Lệnh quét thủ công hoặc cập nhật cấu hình được kích hoạt! Tiến hành quét...")
            trigger_scan_event.clear()
        except asyncio.TimeoutError:
            log.info("Hết thời gian chờ định kỳ, tự động quét...")
            pass
            
        try:
            await asyncio.to_thread(scanner.job)
            await broadcast({"type": "matches", "data": load_matches_file()})
        except Exception as e:
            log.error(f"Lỗi khi quét nến: {e}")

# ── WebSocket broadcaster ─────────────────────────────────────────────────────

async def broadcast(data: dict):
    dead = set()
    for ws in connected_clients:
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    connected_clients.difference_update(dead)

async def position_broadcast_loop():
    while True:
        await asyncio.sleep(2)
        if not binance_client:
            continue
        try:
            positions = fetch_open_positions()
            await broadcast({"type": "positions", "data": positions})
        except Exception as e:
            log.exception("broadcast loop: %s", e)

# ── App lifecycle ─────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    global binance_client
    creds = load_saved_credentials()
    if creds.get("api_key") and creds.get("api_secret"):
        try:
            client = Client(creds["api_key"], creds["api_secret"])
            client.futures_account()
            binance_client = client
            log.info("Auto-connected to Binance Futures using saved credentials")
        except Exception as e:
            log.warning("Auto-connect failed: %s", e)
            
    t1 = asyncio.create_task(position_broadcast_loop())
    t2 = asyncio.create_task(trailing_stop_loop())
    t3 = asyncio.create_task(scanner_loop())
    yield
    t1.cancel()
    t2.cancel()
    t3.cancel()

app = FastAPI(lifespan=lifespan)

# ── REST endpoints ────────────────────────────────────────────────────────────

class Credentials(BaseModel):
    api_key: str
    api_secret: str

class GlobalSettings(BaseModel):
    default_sl: float
    gap: float
    default_pnl: float

class PositionOverride(BaseModel):
    symbol: str
    default_sl: float
    gap: float
    default_pnl: float

class TrailingToggle(BaseModel):
    enabled: bool

class ScannerConfig(BaseModel):
    max_price_usdt: float
    timeframe: str
    limit_candles: int
    interval_minutes: int

@app.delete("/api/saved-credentials")
async def delete_saved_credentials():
    if CREDENTIALS_FILE.exists():
        CREDENTIALS_FILE.unlink()
    return {"ok": True}

@app.post("/api/disconnect")
async def disconnect_api():
    global binance_client
    binance_client = None
    return {"ok": True}

@app.get("/api/saved-credentials")
async def get_saved_credentials():
    creds = load_saved_credentials()
    secret = creds.get("api_secret", "")
    masked = (secret[:4] + "*" * (len(secret) - 4)) if len(secret) > 4 else ("*" * len(secret))
    return {"api_key": creds.get("api_key", ""), "api_secret_masked": masked, "has_saved": bool(creds.get("api_key"))}

@app.post("/api/connect")
async def connect(creds: Credentials):
    global binance_client
    try:
        client = Client(creds.api_key, creds.api_secret)
        client.futures_account()
        binance_client = client
        save_credentials(creds.api_key, creds.api_secret)
        return {"ok": True, "message": "Đã kết nối & lưu thông tin"}
    except BinanceAPIException as e:
        return {"ok": False, "message": str(e)}
    except Exception as e:
        return {"ok": False, "message": f"Lỗi kết nối: {e}"}

@app.post("/api/trailing-stop")
async def set_trailing_stop(body: TrailingToggle):
    global trailing_stop_on
    trailing_stop_on = body.enabled
    if not body.enabled and binance_client:
        for sym in list(position_states.keys()):
            cancel_sl_order(sym)
        position_states.clear()
    await broadcast({"type": "trailing_stop", "enabled": trailing_stop_on})
    return {"ok": True, "enabled": trailing_stop_on}

@app.post("/api/settings")
async def update_global_settings(settings: GlobalSettings):
    global global_settings
    global_settings = settings.model_dump()
    if binance_client and trailing_stop_on:
        for sym in list(position_states.keys()):
            cancel_sl_order(sym)
        position_states.clear()
    return {"ok": True}

@app.post("/api/position-override")
async def set_position_override(body: PositionOverride):
    sym = body.symbol
    position_overrides[sym] = body.model_dump(exclude={"symbol"})
    if sym in position_states:
        cancel_sl_order(sym)
        position_states.pop(sym, None)
    return {"ok": True}

@app.delete("/api/position-override/{symbol}")
async def delete_position_override(symbol: str):
    position_overrides.pop(symbol, None)
    if symbol in position_states:
        cancel_sl_order(symbol)
        position_states.pop(symbol, None)
    return {"ok": True}

class ClosePosition(BaseModel):
    symbol: str
    side: str
    qty: float

@app.post("/api/close-position")
async def close_position_manual(body: ClosePosition):
    if not binance_client:
        return {"ok": False, "message": "Chưa kết nối Binance"}
    try:
        cancel_sl_order(body.symbol)
        position_states.pop(body.symbol, None)
        close_position_market(body.symbol, body.side, body.qty)
        return {"ok": True, "message": f"Đã đóng lệnh {body.symbol}"}
    except Exception as e:
        return {"ok": False, "message": str(e)}

@app.get("/api/status")
async def status():
    return {
        "connected": binance_client is not None,
        "trailing_stop_on": trailing_stop_on,
        "global_settings": global_settings,
    }

# ── Scanner REST endpoints ────────────────────────────────────────────────────

@app.get("/api/scanner-config")
async def get_scanner_config():
    return scanner.load_config()

@app.post("/api/scanner-config")
async def update_scanner_config(cfg: ScannerConfig):
    try:
        new_config = cfg.model_dump()
        with open(scanner.CONFIG_FILE, 'w') as f:
            json.dump(new_config, f, indent=4)
        # Wake up scanner immediately
        trigger_scan_event.set()
        return {"status": "success", "config": new_config}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/matches")
async def get_matches():
    return load_matches_file()

@app.post("/api/scan/now")
async def trigger_scan_now():
    trigger_scan_event.set()
    return {"status": "success", "message": "Đã ra lệnh quét nến đỏ ngay lập tức."}

# ── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.add(ws)
    # Send current state
    await ws.send_json({"type": "trailing_stop", "enabled": trailing_stop_on})
    await ws.send_json({"type": "matches", "data": load_matches_file()})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        connected_clients.discard(ws)

# ── Home ──────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    try:
        with open("templates/index.html", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return HTMLResponse("templates/index.html not found", status_code=404)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
