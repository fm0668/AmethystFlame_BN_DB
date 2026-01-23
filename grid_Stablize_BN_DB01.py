import asyncio
import websockets
import json
import logging
import hmac
import hashlib
import time
import ccxt
import math
import os
import asyncio
import uuid
import signal

from risk_manager import RiskEngine

# ==================== 配置 ====================
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

if load_dotenv:
    load_dotenv()

API_KEY = os.getenv("BINANCE_REAL_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_REAL_API_SECRET", "").strip()
TESTNET_API_KEY = os.getenv("BINANCE_TESTNET_API_KEY", "").strip()
TESTNET_API_SECRET = os.getenv("BINANCE_TESTNET_API_SECRET", "").strip()

COIN_NAME = os.getenv("COIN_NAME", "ETH").strip().upper()
CONTRACT_TYPE = os.getenv("CONTRACT_TYPE", "USDC").strip().upper()
GRID_SPACING = float(os.getenv("GRID_SPACING", "0.003"))
INITIAL_QUANTITY = float(os.getenv("INITIAL_QUANTITY", "0.007"))
LEVERAGE = float(os.getenv("LEVERAGE", "20"))
INITIAL_CAPITAL = float(os.getenv("INITIAL_CAPITAL", "0"))
STATUS_LOG_INTERVAL_SEC = float(os.getenv("STATUS_LOG_INTERVAL_SEC", "60"))

WEBSOCKET_URL_REAL = "wss://fstream.binance.com/ws"
WEBSOCKET_URL_TESTNET = "wss://stream.binancefuture.com/ws"

# ==================== 日志配置 ====================
# 获取当前脚本的文件名（不带扩展名）
script_name = os.path.splitext(os.path.basename(__file__))[0]
_script_dir = os.path.dirname(os.path.abspath(__file__))
_log_dir = os.path.join(_script_dir, "log")
os.makedirs(_log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(instance_id)s] - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(
                _log_dir,
                f"{script_name}_{str(os.getenv('INSTANCE_ID') or '').strip() or 'main'}.log",
            )
        ),  # 日志文件
        logging.StreamHandler(),  # 控制台输出
    ],
)
logger = logging.getLogger()

class _InstanceLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            record.instance_id = str(os.getenv("INSTANCE_ID") or "").strip() or "main"
        except Exception:
            record.instance_id = "main"
        return True

try:
    _f = _InstanceLogFilter()
    for _h in list(getattr(logger, "handlers", []) or []):
        try:
            _h.addFilter(_f)
        except Exception:
            pass
except Exception:
    pass

def _safe_read_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}

def _safe_write_json_atomic(path: str, obj: dict) -> bool:
    try:
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        tmp = f"{path}.tmp.{int(time.time() * 1000)}"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)
        return True
    except Exception:
        try:
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
        except Exception:
            pass
        return False

def _disable_instance_autostart(config_path: str, status_dir: str, instance_id: str) -> None:
    sid = str(instance_id or "").strip()
    if not sid:
        return
    try:
        start_flag = os.path.join(status_dir, f"{sid}.start")
        if os.path.exists(start_flag):
            os.remove(start_flag)
    except Exception:
        pass
    try:
        restart_flag = os.path.join(status_dir, f"{sid}.restart")
        if os.path.exists(restart_flag):
            os.remove(restart_flag)
    except Exception:
        pass
    try:
        cfg = _safe_read_json(config_path) or {}
        inst = cfg.get("实例") if isinstance(cfg.get("实例"), dict) else {}
        inst["启用"] = False
        cfg["实例"] = inst
        _safe_write_json_atomic(config_path, cfg)
    except Exception:
        pass

def _ui_state_for_shutdown_reason(reason: str):
    r = str(reason or "").strip().lower()
    if ("take_profit" in r) or ("tp" == r):
        return "take_profit", "已止盈"
    if ("hard_stop" in r) or ("stoploss" in r) or ("stop_loss" in r) or ("sl" == r):
        return "hard_stoploss", "已止损"
    return None, None

def _is_stop_like_ws_order(order: dict) -> bool:
    if not isinstance(order, dict):
        return False
    ot = str(order.get("o") or "").strip().upper()
    o2 = str(order.get("ot") or "").strip().upper()
    t = ot or o2
    if t in {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT"}:
        return True
    sp = order.get("sp")
    try:
        sp_f = float(sp)
    except Exception:
        sp_f = 0.0
    if sp_f > 0:
        return True
    return False

def _stop_reason_from_ws_order(order: dict):
    if not isinstance(order, dict):
        return None
    ot = str(order.get("o") or "").strip().upper()
    o2 = str(order.get("ot") or "").strip().upper()
    t = ot or o2
    if "TAKE_PROFIT" in t:
        return "take_profit"
    sp = order.get("sp")
    try:
        sp_f = float(sp)
    except Exception:
        sp_f = 0.0
    if sp_f > 0:
        return "hard_stoploss"
    if "STOP" in t:
        return "hard_stoploss"
    return None

def _normalize_account_mode(value) -> str:
    s = str(value or "").strip().lower()
    if s in {"testnet", "paper", "sim", "sandbox", "测试网", "测试", "模拟", "仿真", "测试网络", "测试网路"}:
        return "testnet"
    if s in {"real", "mainnet", "live", "实盘", "正式"}:
        return "real"
    return "real"

def _get_account_mode(config_path: str) -> str:
    cfg = _safe_read_json(config_path)
    return _normalize_account_mode(
        cfg.get("账户模式") or cfg.get("交易环境") or cfg.get("ACCOUNT_MODE") or cfg.get("account_mode") or cfg.get("环境")
    )

def _safe_float(value, default: float):
    try:
        return float(value)
    except Exception:
        return float(default)

def _get_strategy_params(config_path: str) -> dict:
    cfg = _safe_read_json(config_path)
    pair = cfg.get("交易对")
    pair_coin = None
    pair_quote = None
    if isinstance(pair, dict):
        pair_coin = pair.get("币") or pair.get("coin") or pair.get("base")
        pair_quote = pair.get("计价") or pair.get("quote") or pair.get("settle")
    coin_name = str(
        pair_coin or cfg.get("交易币种") or cfg.get("COIN_NAME") or os.getenv("COIN_NAME", "ETH")
    ).strip().upper() or "ETH"
    contract_type = str(
        pair_quote or cfg.get("合约类型") or cfg.get("CONTRACT_TYPE") or os.getenv("CONTRACT_TYPE", "USDC")
    ).strip().upper() or "USDC"
    sync = cfg.get("同步") if isinstance(cfg.get("同步"), dict) else {}
    rest_sync_interval_sec = _safe_float(
        (sync.get("状态同步间隔秒") if isinstance(sync, dict) else None)
        or cfg.get("REST同步间隔秒")
        or cfg.get("REST_SYNC_INTERVAL_SEC")
        or os.getenv("REST_SYNC_INTERVAL_SEC", "10"),
        10.0,
    )
    order_first_time_sec = _safe_float(
        cfg.get("首次下单等待秒") or cfg.get("ORDER_FIRST_TIME_SEC") or os.getenv("ORDER_FIRST_TIME_SEC", "10"),
        10.0,
    )
    if rest_sync_interval_sec <= 0:
        rest_sync_interval_sec = 10.0
    if order_first_time_sec < 0:
        order_first_time_sec = 0.0
    return {
        "coin_name": coin_name,
        "contract_type": contract_type,
        "rest_sync_interval_sec": float(rest_sync_interval_sec),
        "order_first_time_sec": float(order_first_time_sec),
    }

def _parse_env_bool(value, default: bool = False):
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y", "on", "是"}:
        return True
    if s in {"0", "false", "no", "n", "off", "否"}:
        return False
    return default


class CustomGate(ccxt.binanceusdm):
    def fetch(self, url, method='GET', headers=None, body=None):
        if headers is None:
            headers = {}
        # headers['X-Gate-Channel-Id'] = 'laohuoji'
        # headers['Accept'] = 'application/json'
        # headers['Content-Type'] = 'application/json'
        return super().fetch(url, method, headers, body)


# ==================== 网格交易机器人 ====================
class GridTradingBot:
    def __init__(self, api_key, api_secret, coin_name, contract_type, grid_spacing, initial_quantity, leverage, account_mode: str, rest_sync_interval_sec: float, order_first_time_sec: float):
        self.lock = asyncio.Lock()  # 初始化线程锁
        self.api_key = api_key
        self.api_secret = api_secret
        self.coin_name = coin_name
        self.contract_type = contract_type  # 合约类型：USDT 或 USDC
        self.grid_spacing = grid_spacing
        self.initial_quantity = initial_quantity
        self.leverage = leverage
        self.rest_sync_interval_sec = float(rest_sync_interval_sec or 0.0) if rest_sync_interval_sec is not None else 0.0
        if self.rest_sync_interval_sec <= 0:
            self.rest_sync_interval_sec = 10.0
        self.order_first_time_sec = float(order_first_time_sec or 0.0) if order_first_time_sec is not None else 0.0
        if self.order_first_time_sec < 0:
            self.order_first_time_sec = 0.0
        self.account_mode = str(account_mode or "").strip().lower()
        self.websocket_url = WEBSOCKET_URL_TESTNET if self.account_mode in {"testnet", "paper", "sim"} else WEBSOCKET_URL_REAL
        self.exchange = self._initialize_exchange()  # 初始化交易所
        self.ccxt_symbol = f"{coin_name}/{contract_type}:{contract_type}"  # 动态生成交易对
        self.contract_size = 1.0
        self.min_order_cost = None
        self._get_price_precision()

        self.strategy_config_path = os.getenv("STRATEGY_CONFIG_PATH", os.path.join(_script_dir, "config.json"))
        self.risk_engine = RiskEngine(self, self.strategy_config_path)
        self.instance_id = str(os.getenv("INSTANCE_ID") or os.path.splitext(os.path.basename(self.strategy_config_path))[0] or "instance").strip()
        self._status_dir = os.path.join(_script_dir, "status")
        self._status_file_path = os.path.join(self._status_dir, f"{self.instance_id}.json")
        self._stop_flag_path = os.path.join(self._status_dir, f"{self.instance_id}.stop")
        self._last_ws_msg_ts = 0.0
        self._strategy_config_version = 0
        self._force_orders_resync = True
        self.direction = None
        self._hedge_mode = None
        try:
            if self.risk_engine.reload_config(force=True):
                self._strategy_config_version = int(getattr(self.risk_engine, "_config_version", 0) or 0)
        except Exception as e:
            logger.error(f"加载策略配置失败: {e}")

        self.long_tp_quantity = 0
        self.short_tp_quantity = 0
        self.long_add_quantity = float(self.initial_quantity)
        self.short_add_quantity = float(self.initial_quantity)
        self.long_position = 0  # 多头持仓 ws监控
        self.short_position = 0  # 空头持仓 ws监控
        self.last_long_order_time = 0  # 上次多头挂单时间
        self.last_short_order_time = 0  # 上次空头挂单时间
        self.buy_long_orders = 0.0  # 多头买入剩余挂单数量
        self.sell_long_orders = 0.0  # 多头卖出剩余挂单数量
        self.sell_short_orders = 0.0  # 空头卖出剩余挂单数量
        self.buy_short_orders = 0.0  # 空头买入剩余挂单数量
        self.last_position_update_time = 0  # 上次持仓更新时间
        self.last_orders_update_time = 0  # 上次订单更新时间
        self.last_ticker_update_time = 0  # ticker 时间限速
        self.latest_price = 0  # 最新价格
        self.best_bid_price = None  # 最佳买价
        self.best_ask_price = None  # 最佳卖价
        self.balance = {}  # 用于存储合约账户余额
        self.mid_price_long = 0  # long 中间价
        self.lower_price_long = 0  # long 网格上
        self.upper_price_long = 0  # long 网格下
        self.mid_price_short = 0  # short 中间价
        self.lower_price_short = 0  # short 网格上
        self.upper_price_short = 0  # short 网格下
        self._order_event_eval_task = None
        self.listenKey = self.get_listen_key()  # 获取初始 listenKey
        self.shutdown_event = asyncio.Event()
        self._ws = None
        self._open_orders_cache = None
        self._open_orders_cache_ts = 0.0
        self._last_long_grid_action_ts = 0.0
        self._last_short_grid_action_ts = 0.0
        self._grid_action_cooldown_sec = 1.2
        self._last_postonly_reject_ts_long = 0.0
        self._last_postonly_reject_ts_short = 0.0
        self._postonly_reject_cooldown_sec = 2.0
        self.initial_capital = float(INITIAL_CAPITAL or 0.0)
        self._start_time = time.time()

        self.total_fills = 0
        self.buy_fills = 0
        self.sell_fills = 0

        self._last_equity = None
        self._last_equity_ts = 0.0
        self._equity_peak = None
        self._max_drawdown_ratio = 0.0
        self._prev_equity_for_return = None
        self._returns = []
        self._instance_equity_peak = None
        self._instance_max_drawdown_ratio = 0.0
        self._instance_last_equity = None
        self._instance_last_equity_ts = 0.0
        self._status_log_interval_sec = float(STATUS_LOG_INTERVAL_SEC or 60.0)
        self._trail_peak_price_long = None
        self._trail_trough_price_short = None
        self._trail_anchor_entry_long = None
        self._trail_anchor_entry_short = None
        self._last_error_msg = None
        self._last_error_ts = 0.0
        self._err_rl = {}

        self.is_grid_stopped = False
        self._last_risk_eval_ts = 0.0
        self._hedge_task = None
        self._stop_order_id = None
        self._last_stop_update_ts = 0.0
        self.instance_realized_pnl = 0.0
        self.instance_fees = 0.0
        self.instance_fees_by_asset = {}
        self._last_trade_id_seen = set()
        self._last_trade_id_seen_max = 5000

        self._refresh_position_mode()
        self._apply_runtime_settings_from_config()

    def _safe_json_dump(self, obj) -> str:
        try:
            return json.dumps(obj, ensure_ascii=False, sort_keys=False)
        except Exception:
            try:
                return json.dumps({}, ensure_ascii=False)
            except Exception:
                return "{}"

    def _build_status_payload(self):
        cfg = {}
        try:
            cfg = self.risk_engine.get_config() or {}
        except Exception:
            cfg = {}
        allocated = self._safe_float(cfg.get("ALLOCATED_CAPITAL_USDC", cfg.get("ALLOCATED_CAPITAL_USDT", 0.0))) or 0.0
        active = str(self.direction or "long").strip().lower()
        if active not in {"long", "short"}:
            active = "long"

        snap = None
        try:
            snap = self.get_position_snapshot()
        except Exception:
            snap = None

        pos_amt = 0.0
        entry = None
        unreal = 0.0
        if isinstance(snap, dict):
            pos_amt = float(((snap.get(active) or {}).get("amt") or 0.0))
            entry = self._safe_float((snap.get(active) or {}).get("entry_price"))
            unreal = float(self._safe_float((snap.get(active) or {}).get("pnl")) or 0.0)

        equity = None
        if float(allocated) > 0:
            equity = float(allocated) + float(self.instance_realized_pnl or 0.0) - float(self.instance_fees or 0.0) + float(unreal)
        pnl = None
        if equity is not None and float(allocated) > 0:
            pnl = float(equity) - float(allocated)

        now = time.time()
        state = "stopped" if bool(getattr(self, "is_grid_stopped", False)) else "running"
        if self.shutdown_event.is_set():
            s, _ = _ui_state_for_shutdown_reason(getattr(self, "_shutdown_reason", None))
            state = s or "shutdown"

        stop_price = None
        try:
            stop_price = self._compute_trailing_stop_price(active, float(entry or 0.0), float(self.latest_price or 0.0), cfg)
        except Exception:
            stop_price = None

        return {
            "instance_id": self.instance_id,
            "ts": float(now),
            "alive": True,
            "health": {
                "state": state,
                "last_error": getattr(self, "_last_error_msg", None),
                "last_ws_ts": float(self._last_ws_msg_ts or 0.0),
                "last_ticker_ts": float(self.last_ticker_update_time or 0.0),
                "last_rest_pos_sync_ts": float(self.last_position_update_time or 0.0),
                "last_rest_orders_sync_ts": float(self.last_orders_update_time or 0.0),
            },
            "config_digest": {
                "config_path": str(self.strategy_config_path),
                "config_version": int(getattr(self, "_strategy_config_version", 0) or 0),
                "direction": active,
                "symbol": str(self.ccxt_symbol),
                "account_mode": str(self.account_mode),
                "maker_only": bool(cfg.get("MAKER_ONLY", False)),
                "enabled": bool(((_safe_read_json(self.strategy_config_path) or {}).get("实例") or {}).get("启用", True)),
            },
            "accounting": {
                "allocated_usdt": float(allocated),
                "equity_usdt": None if equity is None else float(equity),
                "pnl_usdt": None if pnl is None else float(pnl),
                "max_drawdown_ratio": float(self._instance_max_drawdown_ratio or 0.0),
                "fees_usdt": float(self.instance_fees or 0.0),
                "realized_pnl_usdt": float(self.instance_realized_pnl or 0.0),
                "unrealized_pnl_usdt": float(unreal),
            },
            "position": {
                "side": active,
                "amount": float(pos_amt),
                "entry_price": entry,
                "mark_price": float(self.latest_price or 0.0),
            },
            "orders": {
                "buy_long": float(self.buy_long_orders or 0.0),
                "sell_long": float(self.sell_long_orders or 0.0),
                "sell_short": float(self.sell_short_orders or 0.0),
                "buy_short": float(self.buy_short_orders or 0.0),
            },
            "risk": {
                "hard_stop_price": float(self._safe_float(cfg.get("HARD_STOPLOSS_PRICE", 0.0)) or 0.0),
                "trailing_stop_enabled": bool(cfg.get("TRAILING_STOP_ENABLED", False)),
                "current_stop_price": stop_price,
            },
        }

    def _write_status_file(self):
        try:
            os.makedirs(self._status_dir, exist_ok=True)
        except Exception:
            return
        payload = self._build_status_payload()
        data = self._safe_json_dump(payload)
        tmp = f"{self._status_file_path}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(data)
            os.replace(tmp, self._status_file_path)
        except Exception:
            try:
                with open(self._status_file_path, "w", encoding="utf-8") as f:
                    f.write(data)
            except Exception:
                pass

    async def status_file_loop(self):
        while not self.shutdown_event.is_set():
            try:
                try:
                    if os.path.exists(self._stop_flag_path) and (not getattr(self, "_shutting_down", False)):
                        if not bool(getattr(self, "_stop_flag_seen", False)):
                            self._stop_flag_seen = True
                            logger.info("检测到停止标记，开始优雅退出")
                        await self.shutdown("stop_flag")
                        break
                except Exception:
                    pass
                cfg_raw = _safe_read_json(self.strategy_config_path) or {}
                inst = cfg_raw.get("实例") if isinstance(cfg_raw.get("实例"), dict) else {}
                enabled = inst.get("启用", True) if isinstance(inst, dict) else True
                if enabled is False and (not getattr(self, "_shutting_down", False)):
                    if not bool(getattr(self, "_disabled_seen", False)):
                        self._disabled_seen = True
                        logger.info("检测到实例停用，开始优雅退出")
                    await self.shutdown("disabled")
                    break
                self._write_status_file()
            except Exception:
                pass
            await asyncio.sleep(1.0)

    def _normalize_direction(self, value):
        s = str(value or "").strip().lower()
        if s in {"做多", "多", "long", "l", "buy"}:
            return "long"
        if s in {"做空", "空", "short", "s", "sell"}:
            return "short"
        return None

    def _refresh_position_mode(self):
        try:
            position_mode = self.exchange.fetch_position_mode(symbol=self.ccxt_symbol)
            self._hedge_mode = bool((position_mode or {}).get("hedged", False))
        except Exception:
            self._hedge_mode = None

    def _apply_runtime_settings_from_config(self):
        cfg = {}
        try:
            cfg = self.risk_engine.get_config() or {}
        except Exception:
            cfg = {}

        env_override = os.getenv("STRATEGY_DIRECTION", "").strip()
        d = self._normalize_direction(env_override) or self._normalize_direction(cfg.get("DIRECTION")) or self._normalize_direction(cfg.get("方向"))
        if d is None:
            d = "long"
        if self.direction != d:
            self.direction = d
            self._force_orders_resync = True

        try:
            cd = float(cfg.get("GRID_ACTION_COOLDOWN_SEC", getattr(self, "_grid_action_cooldown_sec", 1.2)) or 0.0)
        except Exception:
            cd = getattr(self, "_grid_action_cooldown_sec", 1.2)
        self._grid_action_cooldown_sec = max(0.0, float(cd))

        try:
            itv = float(cfg.get("STATUS_LOG_INTERVAL_SEC", getattr(self, "_status_log_interval_sec", 60.0)) or 60.0)
        except Exception:
            itv = getattr(self, "_status_log_interval_sec", 60.0)
        if itv > 0:
            self._status_log_interval_sec = float(itv)

        try:
            self._apply_leverage_from_config()
        except Exception:
            pass
        try:
            self._apply_margin_mode_from_config()
        except Exception:
            pass

    def _apply_margin_mode_from_config(self, force: bool = False):
        if getattr(self, "_shutting_down", False) or self.shutdown_event.is_set():
            return
        now = time.time()
        backoff_until = float(getattr(self, "_margin_mode_backoff_until_ts", 0.0) or 0.0)
        if not force and now < backoff_until:
            return
        cfg_raw = _safe_read_json(self.strategy_config_path) or {}
        mm = str(cfg_raw.get("保证金模式") or "").strip()
        if mm not in {"逐仓", "全仓"}:
            mm = "逐仓"
        desired = "isolated" if mm == "逐仓" else "cross"
        if (not force) and str(getattr(self, "_applied_margin_mode", "") or "").strip().lower() == desired:
            return
        market_id = self._raw_market_id()
        if not market_id:
            return
        try:
            if hasattr(self.exchange, "set_margin_mode"):
                try:
                    self.exchange.set_margin_mode(desired, self.ccxt_symbol)
                except Exception:
                    margin_type = "ISOLATED" if desired == "isolated" else "CROSSED"
                    self.exchange.fapiPrivatePostMarginType({"symbol": market_id, "marginType": margin_type})
            else:
                margin_type = "ISOLATED" if desired == "isolated" else "CROSSED"
                self.exchange.fapiPrivatePostMarginType({"symbol": market_id, "marginType": margin_type})
            self._applied_margin_mode = desired
            logger.info(f"已设置保证金模式: {mm}")
        except Exception as e:
            msg = str(e)
            if ("No need to change margin type" in msg) or ("-4046" in msg):
                self._applied_margin_mode = desired
                logger.info(f"保证金模式无需变更: {mm}")
                return
            if ("\"code\":-1000" in msg) or ("code': -1000" in msg) or ("code=-1000" in msg):
                self._margin_mode_backoff_until_ts = now + 120.0
            else:
                self._margin_mode_backoff_until_ts = now + 30.0
            logger.error(f"设置保证金模式失败: {e}")

    def _apply_leverage_from_config(self, force: bool = False):
        if getattr(self, "_shutting_down", False) or self.shutdown_event.is_set():
            return
        now = time.time()
        backoff_until = float(getattr(self, "_leverage_backoff_until_ts", 0.0) or 0.0)
        if not force and now < backoff_until:
            return
        cfg_raw = _safe_read_json(self.strategy_config_path) or {}
        grid = cfg_raw.get("网格") if isinstance(cfg_raw.get("网格"), dict) else {}
        lev = None
        if isinstance(grid, dict):
            lev = grid.get("杠杆倍数")
        if lev is None:
            return
        try:
            lev_i = int(float(lev))
        except Exception:
            return
        if lev_i <= 0:
            return
        if lev_i > 125:
            lev_i = 125
        if (not force) and int(getattr(self, "_applied_leverage", 0) or 0) == lev_i:
            return
        market_id = self._raw_market_id()
        if not market_id:
            return
        try:
            if hasattr(self.exchange, "set_leverage"):
                try:
                    self.exchange.set_leverage(lev_i, self.ccxt_symbol)
                except Exception:
                    self.exchange.fapiPrivatePostLeverage({"symbol": market_id, "leverage": lev_i})
            else:
                self.exchange.fapiPrivatePostLeverage({"symbol": market_id, "leverage": lev_i})
            self.leverage = lev_i
            self._applied_leverage = lev_i
            logger.info(f"已设置杠杆倍数: {lev_i}x")
        except Exception as e:
            msg = str(e)
            if ("\"code\":-1000" in msg) or ("code': -1000" in msg) or ("code=-1000" in msg):
                self._leverage_backoff_until_ts = now + 120.0
            else:
                self._leverage_backoff_until_ts = now + 30.0
            logger.error(f"设置杠杆失败: {e}")

    def _position_amount_from_ccxt_position(self, position: dict) -> float:
        if not isinstance(position, dict):
            return 0.0
        info = position.get("info") or {}
        contracts = position.get("contracts")
        if contracts is None:
            contracts = info.get("contracts")

        contract_size = position.get("contractSize")
        if contract_size is None:
            contract_size = (position.get("info") or {}).get("contractSize")
        try:
            contract_size_f = float(contract_size) if contract_size is not None else 1.0
        except Exception:
            contract_size_f = 1.0
        if contract_size_f <= 0:
            contract_size_f = 1.0
        if contracts is not None:
            try:
                contracts_f = float(contracts or 0.0)
            except Exception:
                contracts_f = 0.0
            return abs(float(contracts_f)) * float(contract_size_f)

        position_amt = info.get("positionAmt")
        if position_amt is not None:
            try:
                amt_f = float(position_amt)
            except Exception:
                amt_f = 0.0
            if float(contract_size_f) != 1.0:
                return abs(float(amt_f)) * float(contract_size_f)
            return abs(float(amt_f))

        return 0.0

    def _position_side_from_ccxt_position(self, position: dict):
        if not isinstance(position, dict):
            return None
        side = str(position.get("side") or "").strip().lower()
        if side in {"long", "short"}:
            return side
        info = position.get("info") or {}
        ps = str(info.get("positionSide") or "").strip().upper()
        if ps == "LONG":
            return "long"
        if ps == "SHORT":
            return "short"
        position_amt = info.get("positionAmt")
        if position_amt is not None:
            try:
                v = float(position_amt)
                if v > 0:
                    return "long"
                if v < 0:
                    return "short"
            except Exception:
                pass
        return None

    def _safe_float(self, v):
        try:
            if v is None:
                return None
            return float(v)
        except Exception:
            return None

    def _set_last_error(self, msg: str):
        s = str(msg or "").strip()
        if not s:
            return
        self._last_error_msg = s
        self._last_error_ts = time.time()

    def _err_rate_limit(self, key: str, interval_sec: float = 10.0) -> bool:
        now = time.time()
        try:
            last = float((self._err_rl or {}).get(key) or 0.0)
        except Exception:
            last = 0.0
        if (now - last) < float(interval_sec or 0.0):
            return False
        try:
            self._err_rl[key] = now
        except Exception:
            pass
        return True

    def round_amount(self, quantity: float):
        q = self._safe_float(quantity)
        if q is None:
            return None
        try:
            q = round(float(q), int(self.amount_precision or 0))
        except Exception:
            q = float(q)
        return float(q)

    def round_amount_down(self, quantity: float):
        q = self._safe_float(quantity)
        if q is None:
            return None
        prec = int(self.amount_precision or 0)
        if prec <= 0:
            return float(math.floor(float(q)))
        scale = 10 ** prec
        v = float(q) * float(scale)
        v2 = math.floor(v + 1e-12)
        out = float(v2) / float(scale)
        if out <= 0:
            return None
        return float(out)

    def _safe_bool(self, v, default: bool = False):
        if v is None:
            return default
        if isinstance(v, bool):
            return bool(v)
        s = str(v).strip().lower()
        if s in {"1", "true", "yes", "y", "on", "是"}:
            return True
        if s in {"0", "false", "no", "n", "off", "否"}:
            return False
        return default

    def _maker_only_enabled(self) -> bool:
        try:
            cfg = self.risk_engine.get_config()
        except Exception:
            cfg = {}
        return self._safe_bool((cfg or {}).get("MAKER_ONLY"), False)

    def _post_only_price(self, side: str, price: float):
        p = self._safe_float(price)
        if p is None or p <= 0:
            return None
        prec = int(self.price_precision or 0)
        scale = 10 ** max(0, prec)
        tick = 1.0 / float(scale) if scale > 0 else 1e-8
        if tick <= 0:
            tick = 1e-8
        bid = self._safe_float(getattr(self, "best_bid_price", None)) or 0.0
        ask = self._safe_float(getattr(self, "best_ask_price", None)) or 0.0
        s = str(side or "").strip().lower()
        if s == "buy":
            if ask > 0 and p >= ask:
                p = float(ask) - float(tick)
            v = float(p) * float(scale)
            v2 = math.floor(v + 1e-12)
            p = float(v2) / float(scale)
            if ask > 0 and p >= ask:
                p = float(ask) - float(tick)
                v = float(p) * float(scale)
                v2 = math.floor(v + 1e-12)
                p = float(v2) / float(scale)
            if p <= 0:
                return None
        elif s == "sell":
            if bid > 0 and p <= bid:
                p = float(bid) + float(tick)
            v = float(p) * float(scale)
            v2 = math.ceil(v - 1e-12)
            p = float(v2) / float(scale)
            if bid > 0 and p <= bid:
                p = float(bid) + float(tick)
                v = float(p) * float(scale)
                v2 = math.ceil(v - 1e-12)
                p = float(v2) / float(scale)
            if p <= 0:
                return None
        else:
            return None
        return float(p)

    def _update_anchor_after_fill(self, position_side: str, fill_price: float):
        ps = str(position_side or "").strip().upper()
        fp = self._safe_float(fill_price)
        if fp is None or fp <= 0:
            fp = self._safe_float(getattr(self, "latest_price", None))
        if fp is None or fp <= 0:
            return
        if ps == "LONG":
            if float(getattr(self, "long_position", 0.0) or 0.0) > 0:
                self.mid_price_long = float(fp)
            else:
                self.mid_price_long = 0.0
        elif ps == "SHORT":
            if float(getattr(self, "short_position", 0.0) or 0.0) > 0:
                self.mid_price_short = float(fp)
            else:
                self.mid_price_short = 0.0

    def usdc_to_amount(self, size_usdc: float, price: float):
        s = self._safe_float(size_usdc)
        p = self._safe_float(price)
        if s is None or p is None or s <= 0 or p <= 0:
            return None
        cs = self._safe_float(getattr(self, "contract_size", 1.0))
        if cs is None or cs <= 0:
            cs = 1.0
        denom = float(p) * float(cs)
        if denom <= 0:
            return None
        qty_raw = float(s) / float(denom)
        qty = self.round_amount(float(qty_raw))
        if qty is None:
            return None
        if float(qty) <= 0:
            if self._err_rate_limit("order_qty_round_to_zero", 15.0):
                self._set_last_error(
                    f"下单金额过小：每格金额={float(s):.6f} 计算数量={float(qty_raw):.6f}，按数量精度={int(self.amount_precision or 0)}舍入后为0；请提高“每格金额”"
                )
            return None
        min_amt = float(self.min_order_amount or 0.0)
        if min_amt > 0 and float(qty) < min_amt:
            min_notional = float(min_amt) * float(denom)
            if min_notional > float(s) * 1.05:
                if self._err_rate_limit("min_order_notional_too_high", 15.0):
                    self._set_last_error(
                        f"下单金额过小：每格金额={float(s):.6f} < 最小下单名义={float(min_notional):.6f}；请提高“每格金额”"
                    )
                return None
            qty2 = self.round_amount(min_amt)
            if qty2 is None or float(qty2) <= 0:
                qty2 = min_amt
            if float(qty2) < min_amt:
                qty2 = min_amt
            qty = qty2
        min_cost = self._safe_float(getattr(self, "min_order_cost", None))
        if min_cost is not None and min_cost > 0:
            min_qty_cost = float(min_cost) / float(denom)
            qty_min = self.round_amount_up(min_qty_cost)
            if qty_min is not None and float(qty_min) > float(qty):
                qty = float(qty_min)
        return float(qty)

    def _get_order_client_id(self, order: dict):
        if not isinstance(order, dict):
            return None
        cid = order.get("clientOrderId")
        if cid:
            return str(cid)
        info = order.get("info") or {}
        cid = info.get("clientOrderId") or info.get("c")
        if cid:
            return str(cid)
        return None

    def _get_order_timestamp_sec(self, order: dict):
        if not isinstance(order, dict):
            return None
        ts = order.get("timestamp")
        info = order.get("info") or {}
        if ts is None:
            ts = info.get("time")
        if ts is None:
            ts = info.get("updateTime")
        if ts is None:
            ts = info.get("transactTime")
        if ts is None:
            ts = info.get("T") or info.get("E")
        try:
            ts_f = float(ts)
        except Exception:
            return None
        if ts_f > 1e12:
            return ts_f / 1000.0
        if ts_f > 1e10:
            return ts_f / 1000.0
        if ts_f > 0:
            return ts_f
        return None

    def _get_open_orders_cached(self, max_age_sec: float = 1.0):
        now = time.time()
        cached = getattr(self, "_open_orders_cache", None)
        ts = float(getattr(self, "_open_orders_cache_ts", 0.0) or 0.0)
        if cached is not None and (now - ts) <= float(max_age_sec or 0.0):
            return cached
        orders = self.exchange.fetch_open_orders(self.ccxt_symbol)
        self._open_orders_cache = orders
        self._open_orders_cache_ts = now
        return orders

    def _refresh_open_orders_cache(self):
        try:
            orders = self.exchange.fetch_open_orders(self.ccxt_symbol)
            self._open_orders_cache = orders
            self._open_orders_cache_ts = time.time()
        except Exception:
            self._open_orders_cache = None
            self._open_orders_cache_ts = time.time()

    def _get_order_position_side(self, order: dict):
        info = (order or {}).get("info") or {}
        ps = info.get("positionSide") or info.get("ps") or order.get("positionSide")
        if ps is None:
            return None
        s = str(ps).strip().upper()
        if s in {"LONG", "SHORT", "BOTH"}:
            return s
        return None

    def _get_order_reduce_only(self, order: dict):
        if not isinstance(order, dict):
            return False
        if bool(order.get("reduceOnly")):
            return True
        info = order.get("info") or {}
        v = info.get("reduceOnly")
        if v is None:
            v = info.get("reduce_only")
        if v is not None:
            return bool(v)
        if bool(info.get("reduceOnly")):
            return True
        if bool(info.get("closePosition")):
            return True

        try:
            ps = self._get_order_position_side(order)
            side = str(order.get("side") or info.get("side") or info.get("S") or "").strip().lower()
            if side in {"buy", "sell"} and ps in {"LONG", "SHORT"}:
                if ps == "LONG" and side == "sell":
                    return True
                if ps == "SHORT" and side == "buy":
                    return True
        except Exception:
            pass
        return False

    def _is_stop_order(self, order: dict) -> bool:
        if not isinstance(order, dict):
            return False
        t = order.get("type")
        info = order.get("info") or {}
        if t is None:
            t = info.get("type") or info.get("o")
        s = str(t or "").strip().lower()
        if not s:
            return False
        return ("stop" in s) or ("take_profit" in s) or (s in {"stop_market", "stop", "stop loss", "stoploss", "stop_loss"})

    def _has_duplicate_add_order(self, side: str, price: float, position_side: str):
        try:
            desired_side = str(side or "").strip().lower()
            desired_ps = str(position_side or "").strip().upper()
            hedged = bool(getattr(self, "_hedge_mode", False))
            if hedged:
                if desired_ps in {"LONG", "SHORT"}:
                    pass
                elif desired_ps.lower() in {"long", "short"}:
                    desired_ps = desired_ps.upper()
                else:
                    return False
            desired_price = round(float(price), int(self.price_precision or 0))
        except Exception:
            return False

        try:
            orders = self._get_open_orders_cached(max_age_sec=0.8)
        except Exception:
            return False

        for o in orders or []:
            try:
                if self._is_stop_order(o):
                    continue
                o_side = str((o or {}).get("side") or "").strip().lower()
                if o_side != desired_side:
                    continue
                if bool(getattr(self, "_hedge_mode", False)):
                    o_ps = self._get_order_position_side(o)
                    if o_ps != desired_ps:
                        continue
                if self._get_order_reduce_only(o):
                    continue
                o_price = o.get("price")
                if o_price is None:
                    o_price = (o.get("info") or {}).get("price")
                if o_price is None:
                    continue
                o_price_f = round(float(o_price), int(self.price_precision or 0))
                if o_price_f != desired_price:
                    continue
                return True
            except Exception:
                continue
        return False

    def _in_grid_action_cooldown(self, side: str):
        now = time.time()
        cd = float(getattr(self, "_grid_action_cooldown_sec", 0.0) or 0.0)
        if cd <= 0:
            return False
        s = str(side or "").strip().lower()
        if s == "long":
            last_ts = float(getattr(self, "_last_long_grid_action_ts", 0.0) or 0.0)
        elif s == "short":
            last_ts = float(getattr(self, "_last_short_grid_action_ts", 0.0) or 0.0)
        else:
            return False
        return (now - last_ts) < cd

    def _mark_grid_action(self, side: str):
        now = time.time()
        s = str(side or "").strip().lower()
        if s == "long":
            self._last_long_grid_action_ts = now
        elif s == "short":
            self._last_short_grid_action_ts = now

    def _mark_postonly_reject(self, side: str):
        now = time.time()
        s = str(side or "").strip().lower()
        if s in {"long", "l"}:
            self._last_postonly_reject_ts_long = now
        elif s in {"short", "s"}:
            self._last_postonly_reject_ts_short = now

    def _recent_postonly_reject(self, side: str) -> bool:
        cd = float(getattr(self, "_postonly_reject_cooldown_sec", 0.0) or 0.0)
        if cd <= 0:
            return False
        now = time.time()
        s = str(side or "").strip().lower()
        if s == "long":
            ts = float(getattr(self, "_last_postonly_reject_ts_long", 0.0) or 0.0)
        elif s == "short":
            ts = float(getattr(self, "_last_postonly_reject_ts_short", 0.0) or 0.0)
        else:
            return False
        return (now - ts) < cd

    def _extract_equity_from_balance_info(self, info: dict):
        currency = str(self.contract_type or "").upper()
        assets = info.get("assets")

        if isinstance(assets, dict):
            a = assets.get(currency) or assets.get(currency.lower()) or assets.get(currency.upper())
            if isinstance(a, dict):
                mb = self._safe_float(a.get("marginBalance"))
                if mb is not None:
                    return mb
                wb = self._safe_float(a.get("walletBalance"))
                up = self._safe_float(a.get("unrealizedProfit"))
                if wb is not None and up is not None:
                    return wb + up
                if wb is not None:
                    return wb

        if isinstance(assets, list):
            for a in assets:
                if str(a.get("asset", "")).upper() != currency:
                    continue
                mb = self._safe_float(a.get("marginBalance"))
                if mb is not None:
                    return mb
                wb = self._safe_float(a.get("walletBalance"))
                up = self._safe_float(a.get("unrealizedProfit"))
                if wb is not None and up is not None:
                    return wb + up
                if wb is not None:
                    return wb

        margin_balance = self._safe_float(info.get("totalMarginBalance"))
        if margin_balance is not None and margin_balance > 0:
            return margin_balance

        wallet = self._safe_float(info.get("totalWalletBalance"))
        unreal = self._safe_float(info.get("totalUnrealizedProfit"))
        if wallet is not None and unreal is not None and (wallet != 0.0 or unreal != 0.0):
            return wallet + unreal
        return None

    def get_dynamic_equity(self):
        """获取当前账户动态权益 (余额 + 未实现盈亏)"""
        balance = self.exchange.fetch_balance(params={"type": "future"})
        info = balance.get("info") or {}

        equity = self._extract_equity_from_balance_info(info)
        if equity is not None:
            return equity

        currency = str(self.contract_type or "").upper()
        total = balance.get("total") or {}
        free = balance.get("free") or {}
        used = balance.get("used") or {}
        total_v = self._safe_float(total.get(currency))
        free_v = self._safe_float(free.get(currency))
        used_v = self._safe_float(used.get(currency))
        if total_v is not None and total_v > 0:
            return total_v
        if free_v is not None or used_v is not None:
            return float(free_v or 0.0) + float(used_v or 0.0)
        if total_v is not None:
            return total_v
        return None

    def _format_duration(self, seconds: float):
        try:
            s = int(max(0.0, float(seconds or 0.0)))
        except Exception:
            s = 0
        h = s // 3600
        m = (s % 3600) // 60
        sec = s % 60
        return f"{h:02d}:{m:02d}:{sec:02d}"

    def _update_equity_metrics(self, equity: float, ts: float):
        try:
            e = float(equity)
        except Exception:
            return

        if self.initial_capital <= 0 and e > 0:
            self.initial_capital = e

        self._last_equity = e
        self._last_equity_ts = float(ts or time.time())

        if self._equity_peak is None:
            self._equity_peak = e
        else:
            self._equity_peak = max(float(self._equity_peak), e)

        peak = float(self._equity_peak or 0.0)
        if peak > 0:
            dd = max(0.0, (peak - e) / peak)
            self._max_drawdown_ratio = max(float(self._max_drawdown_ratio or 0.0), dd)

        if self._prev_equity_for_return is None:
            self._prev_equity_for_return = e
            return

        prev = float(self._prev_equity_for_return or 0.0)
        self._prev_equity_for_return = e
        if prev > 0:
            r = (e / prev) - 1.0
            self._returns.append(float(r))
            if len(self._returns) > 20000:
                self._returns = self._returns[-10000:]

    def _update_instance_equity_metrics(self, equity: float, ts: float):
        try:
            e = float(equity)
        except Exception:
            return
        self._instance_last_equity = e
        self._instance_last_equity_ts = float(ts or time.time())
        if self._instance_equity_peak is None:
            self._instance_equity_peak = e
        else:
            self._instance_equity_peak = max(float(self._instance_equity_peak), e)
        peak = float(self._instance_equity_peak or 0.0)
        if peak > 0:
            dd = max(0.0, (peak - e) / peak)
            self._instance_max_drawdown_ratio = max(float(self._instance_max_drawdown_ratio or 0.0), dd)

    def _compute_sharpe(self):
        rs = list(self._returns or [])
        if len(rs) < 30:
            return None
        mean = sum(rs) / float(len(rs))
        var = 0.0
        for r in rs:
            var += (r - mean) ** 2
        var /= float(len(rs) - 1)
        if var <= 0:
            return None
        std = math.sqrt(var)
        periods_per_year = 365.0 * 24.0 * 3600.0 / max(1.0, float(self._status_log_interval_sec or 60.0))
        return (mean / std) * math.sqrt(periods_per_year)

    async def status_log_loop(self):
        if self._status_log_interval_sec <= 0:
            return
        while not self.shutdown_event.is_set():
            self._apply_runtime_settings_from_config()
            now = time.time()
            equity = None
            instance_equity = None
            cfg = {}
            try:
                cfg = self.risk_engine.get_config() or {}
            except Exception:
                cfg = {}
            allocated = self._safe_float(cfg.get("ALLOCATED_CAPITAL_USDC", cfg.get("ALLOCATED_CAPITAL_USDT", 0.0)))
            if allocated is not None and float(allocated) > 0:
                try:
                    snap = self.get_position_snapshot()
                    active = str(self.direction or "long").strip().lower()
                    if active not in {"long", "short"}:
                        active = "long"
                    unreal = self._safe_float((snap.get(active) or {}).get("pnl")) or 0.0
                    instance_equity = float(allocated) + float(getattr(self, "instance_realized_pnl", 0.0) or 0.0) - float(getattr(self, "instance_fees", 0.0) or 0.0) + float(unreal)
                    equity = float(instance_equity)
                    if float(self.initial_capital or 0.0) <= 0:
                        self.initial_capital = float(allocated)
                    self._update_instance_equity_metrics(float(instance_equity), now)
                except Exception:
                    instance_equity = None
            else:
                instance_equity = None
                equity = None

            if equity is not None:
                self._update_equity_metrics(equity, now)

            runtime = self._format_duration(now - float(getattr(self, "_start_time", now) or now))
            init_cap = float(self.initial_capital or 0.0)
            pnl = None
            if equity is not None and init_cap > 0:
                pnl = float(equity) - init_cap

            annualized = None
            if pnl is not None and init_cap > 0:
                elapsed = max(1.0, now - float(getattr(self, "_start_time", now) or now))
                if elapsed >= 86400.0:
                    years = elapsed / (365.0 * 24.0 * 3600.0)
                    if years > 0:
                        annualized = (pnl / init_cap) / years

            sharpe = self._compute_sharpe()
            if instance_equity is not None:
                max_dd = float(self._instance_max_drawdown_ratio or 0.0)
            else:
                max_dd = float(self._max_drawdown_ratio or 0.0)

            if instance_equity is not None:
                equity_s = f"{float(instance_equity):.6f}"
            else:
                equity_s = "N/A" if equity is None else f"{float(equity):.6f}"
            pnl_s = "N/A" if pnl is None else f"{float(pnl):.6f}"
            annual_s = "N/A" if annualized is None else f"{float(annualized) * 100:.2f}%"
            dd_s = f"{max_dd * 100:.2f}%"
            sharpe_s = "N/A" if sharpe is None else f"{float(sharpe):.2f}"

            logger.info(
                "\n".join(
                    [
                        f"策略运行时长：{runtime}",
                        f"成交笔数：总{self.total_fills}（做多{self.buy_fills}，做空{self.sell_fills}）",
                        f"账户权益：{equity_s}",
                        f"盈亏情况：{pnl_s}",
                        f"预估年化：{annual_s}",
                        f"最大回撤：{dd_s}",
                        f"夏普比率：{sharpe_s}",
                    ]
                )
            )
            await asyncio.sleep(self._status_log_interval_sec)

    def get_position_snapshot(self):
        params = {"type": "future"}
        positions = self.exchange.fetch_positions(params=params)
        out = {
            "long": {"amt": 0.0, "pnl": 0.0, "entry_price": None},
            "short": {"amt": 0.0, "pnl": 0.0, "entry_price": None},
        }
        for position in positions:
            if position.get("symbol") != self.ccxt_symbol:
                continue
            side = self._position_side_from_ccxt_position(position)
            if side not in {"long", "short"}:
                continue
            contracts = self._position_amount_from_ccxt_position(position)
            amt = abs(float(contracts or 0.0))
            pnl = self._safe_float(position.get("unrealizedPnl"))
            if pnl is None:
                pnl = self._safe_float(position.get("unrealizedProfit"))
            info = position.get("info") or {}
            if pnl is None:
                pnl = self._safe_float(info.get("unRealizedProfit"))
            if pnl is None:
                pnl = self._safe_float(info.get("unrealizedProfit"))
            if pnl is None:
                pnl = 0.0
            entry = self._safe_float(position.get("entryPrice"))
            if entry is None:
                entry = self._safe_float((position.get("info") or {}).get("entryPrice"))
            if entry is None:
                entry = self._safe_float(info.get("entryPrice"))

            if side == "long":
                out["long"]["amt"] = max(0.0, float(amt))
                out["long"]["pnl"] = float(pnl)
                out["long"]["entry_price"] = entry
            else:
                out["short"]["amt"] = max(0.0, float(amt))
                out["short"]["pnl"] = float(pnl)
                out["short"]["entry_price"] = entry
        return out

    def _cancel_stop_orders_for_side(self, side: str):
        s = str(side or "").strip().lower()
        if s not in {"long", "short"}:
            return
        required_ps = None
        if bool(getattr(self, "_hedge_mode", False)):
            required_ps = "LONG" if s == "long" else "SHORT"
        try:
            orders = self._get_open_orders_cached(max_age_sec=0.0) or []
        except Exception:
            orders = []
        for o in orders:
            try:
                if not self._is_stop_order(o):
                    continue
                if not self._get_order_reduce_only(o):
                    continue
                if required_ps is not None:
                    ps = self._get_order_position_side(o)
                    if ps != required_ps:
                        continue
                oid = (o or {}).get("id")
                if oid:
                    self.cancel_order(oid)
            except Exception:
                continue
        self._stop_order_id = None
        self._refresh_open_orders_cache()

    def _raw_market_id(self):
        try:
            return (self.exchange.market(self.ccxt_symbol) or {}).get("id")
        except Exception:
            pass
        try:
            return self.ccxt_symbol.split(":")[0].replace("/", "")
        except Exception:
            return None

    def _raw_purge_stop_like_orders(self, desired_side: str, required_position_side: str = None, limit: int = 200):
        if not hasattr(self.exchange, "fapiPrivateDeleteOrder"):
            return 0
        market_id = self._raw_market_id()
        if not market_id:
            return 0
        orders = []
        if hasattr(self.exchange, "fapiPrivateGetOpenOrders"):
            try:
                orders = self.exchange.fapiPrivateGetOpenOrders({"symbol": market_id})
            except Exception:
                orders = []
        if not isinstance(orders, list):
            orders = []
        if not orders and hasattr(self.exchange, "fapiPrivateGetAllOrders"):
            try:
                orders = self.exchange.fapiPrivateGetAllOrders({"symbol": market_id, "limit": 200})
            except Exception:
                orders = []
        if not isinstance(orders, list):
            orders = []
        ds = str(desired_side or "").strip().upper()
        rps = str(required_position_side).strip().upper() if required_position_side is not None else None
        cancelled = 0
        for o in orders:
            if cancelled >= int(limit or 0):
                break
            try:
                raw = o or {}
                status = str(raw.get("status") or "").strip().upper()
                if status and status not in {"NEW", "PARTIALLY_FILLED"}:
                    continue
                t = str(raw.get("type") or "").upper()
                stop_price = raw.get("stopPrice")
                close_pos = str(raw.get("closePosition") or "").strip().lower() in {"true", "1", "yes"}
                if ("STOP" not in t) and ("TAKE_PROFIT" not in t) and (not close_pos):
                    try:
                        if stop_price is None or float(stop_price) <= 0:
                            continue
                    except Exception:
                        continue
                side = str((o or {}).get("side") or "").strip().upper()
                if ds and side and side != ds:
                    continue
                if rps is not None:
                    ps = str(raw.get("positionSide") or "").strip().upper()
                    if ps and ps not in {rps, "BOTH"}:
                        continue
                oid = raw.get("orderId")
                if not oid:
                    continue
                self.exchange.fapiPrivateDeleteOrder({"symbol": market_id, "orderId": oid})
                cancelled += 1
            except Exception:
                continue
        return cancelled

    def _raw_cancel_all_open_orders_for_symbol(self):
        market_id = self._raw_market_id()
        if not market_id:
            return False
        if hasattr(self.exchange, "fapiPrivateDeleteAllOpenOrders"):
            try:
                self.exchange.fapiPrivateDeleteAllOpenOrders({"symbol": market_id})
                return True
            except Exception:
                return False
        if hasattr(self.exchange, "fapiPrivateDeleteAllOpenOrders"):
            try:
                self.exchange.fapiPrivateDeleteAllOpenOrders({"symbol": market_id})
                return True
            except Exception:
                return False
        return False

    def _compute_trailing_stop_price(self, side: str, entry_price: float, current_price: float, cfg: dict):
        s = str(side or "").strip().lower()
        ep = self._safe_float(entry_price)
        cp = self._safe_float(current_price)
        if ep is None or cp is None or ep <= 0 or cp <= 0:
            return None

        if s == "long":
            anchor = self._safe_float(getattr(self, "_trail_anchor_entry_long", None))
            if anchor is None or anchor <= 0 or abs(float(anchor) - float(ep)) / float(ep) >= 0.01:
                setattr(self, "_trail_anchor_entry_long", float(ep))
                setattr(self, "_trail_peak_price_long", float(cp))
            peak = self._safe_float(getattr(self, "_trail_peak_price_long", None))
            if peak is None or peak <= 0:
                peak = float(cp)
            if float(cp) > float(peak):
                peak = float(cp)
                setattr(self, "_trail_peak_price_long", float(peak))
        else:
            anchor = self._safe_float(getattr(self, "_trail_anchor_entry_short", None))
            if anchor is None or anchor <= 0 or abs(float(anchor) - float(ep)) / float(ep) >= 0.01:
                setattr(self, "_trail_anchor_entry_short", float(ep))
                setattr(self, "_trail_trough_price_short", float(cp))
            trough = self._safe_float(getattr(self, "_trail_trough_price_short", None))
            if trough is None or trough <= 0:
                trough = float(cp)
            if float(cp) < float(trough):
                trough = float(cp)
                setattr(self, "_trail_trough_price_short", float(trough))

        candidates = []
        base_ratio = self._safe_float(cfg.get("TRAILING_STOP_BASE_STOP_RATIO", 0.0))
        if base_ratio is not None and base_ratio > 0:
            if s == "long":
                candidates.append(ep * (1.0 - float(base_ratio)))
            else:
                candidates.append(ep * (1.0 + float(base_ratio)))

        ladder = cfg.get("TRAILING_STOP_LADDER") or []
        if isinstance(ladder, list):
            if s == "long":
                profit_ratio = (cp / ep) - 1.0
            else:
                profit_ratio = (ep / cp) - 1.0
            best = None
            for item in ladder:
                if not isinstance(item, dict):
                    continue
                tr = self._safe_float(item.get("trigger_ratio"))
                sr = self._safe_float(item.get("stop_ratio"))
                if tr is None or sr is None:
                    continue
                if profit_ratio >= float(tr):
                    best = float(sr) if best is None else max(float(best), float(sr))
            if best is not None:
                if s == "long":
                    candidates.append(ep * (1.0 + float(best)))
                else:
                    candidates.append(ep * (1.0 - float(best)))

        pb_ladder = cfg.get("TRAILING_PULLBACK_LADDER") or []
        if isinstance(pb_ladder, list) and pb_ladder:
            if s == "long":
                pr = (float(peak) / float(ep)) - 1.0
            else:
                pr = (float(ep) / float(trough)) - 1.0
            best_pb = None
            for item in pb_ladder:
                if not isinstance(item, dict):
                    continue
                tr = self._safe_float(item.get("trigger_ratio"))
                pb = self._safe_float(item.get("pullback_ratio"))
                if tr is None or pb is None:
                    continue
                if pr >= float(tr):
                    best_pb = float(pb) if best_pb is None else min(float(best_pb), float(pb))
            if best_pb is not None and float(best_pb) > 0:
                if s == "long":
                    candidates.append(float(peak) * (1.0 - float(best_pb)))
                else:
                    candidates.append(float(trough) * (1.0 + float(best_pb)))

        hs_price = self._safe_float(cfg.get("HARD_STOPLOSS_PRICE", 0.0))
        if hs_price is not None and hs_price > 0:
            candidates.append(float(hs_price))

        if not candidates:
            return None

        if s == "long":
            stop_price = max(float(x) for x in candidates if x is not None and float(x) > 0)
            if stop_price >= float(cp):
                stop_price = float(cp) * 0.999
        else:
            stop_price = min(float(x) for x in candidates if x is not None and float(x) > 0)
            if stop_price <= float(cp):
                stop_price = float(cp) * 1.001

        stop_price = round(float(stop_price), int(self.price_precision or 0))
        if stop_price <= 0:
            return None
        return float(stop_price)

    def _upsert_stop_market(self, side: str, qty: float, stop_price: float) -> str:
        s = str(side or "").strip().lower()
        q = self._safe_float(qty)
        sp = self._safe_float(stop_price)
        if s not in {"long", "short"} or q is None or sp is None:
            return "invalid"
        if q <= 0 or sp <= 0:
            return "invalid"
        now = time.time()
        if (now - float(getattr(self, "_last_stop_update_ts", 0.0) or 0.0)) < 1.0:
            return "throttled"
        self._last_stop_update_ts = now

        required_ps = None
        if bool(getattr(self, "_hedge_mode", False)):
            required_ps = "LONG" if s == "long" else "SHORT"

        try:
            orders = self._get_open_orders_cached(max_age_sec=0.0) or []
        except Exception:
            orders = []
        desired_o_side = "sell" if s == "long" else "buy"
        desired_stop = round(float(sp), int(self.price_precision or 0))
        candidates = []
        for o in orders:
            try:
                if not self._is_stop_order(o):
                    continue
                o_side = str((o or {}).get("side") or "").strip().lower()
                if o_side != desired_o_side:
                    continue
                if required_ps is not None:
                    ps = self._get_order_position_side(o)
                    if ps not in {required_ps, None, "BOTH"}:
                        continue
                info = (o or {}).get("info") or {}
                ex_stop = self._safe_float(info.get("stopPrice")) or self._safe_float(info.get("sp"))
                ex_stop = round(float(ex_stop or 0.0), int(self.price_precision or 0))
                candidates.append({"order": o, "stop": ex_stop})
            except Exception:
                continue

        if len(candidates) == 1 and float(candidates[0]["stop"] or 0.0) == float(desired_stop):
            return "ok"

        last_purge = float(getattr(self, "_last_stop_purge_ts", 0.0) or 0.0)
        if (now - last_purge) >= 3.0 and candidates:
            setattr(self, "_last_stop_purge_ts", now)
            cancelled = 0
            for it in candidates:
                if cancelled >= 60:
                    break
                try:
                    oid = (it.get("order") or {}).get("id")
                    if oid:
                        self.cancel_order(oid)
                        cancelled += 1
                except Exception:
                    continue

        if bool(getattr(self, "_hedge_mode", False)) and required_ps is not None:
            params = {"stopPrice": float(sp), "closePosition": True, "positionSide": required_ps}
        else:
            params = {"stopPrice": float(sp), "closePosition": True}
        try:
            self.exchange.create_order(self.ccxt_symbol, "STOP_MARKET", desired_o_side, float(q), None, params)
            self._refresh_open_orders_cache()
            return "ok"
        except Exception as e:
            msg = str(e)
            if ("-2021" in msg) or ("immediately trigger" in msg):
                try:
                    px = None
                    if s == "long":
                        px = self._safe_float(getattr(self, "best_bid_price", None))
                    else:
                        px = self._safe_float(getattr(self, "best_ask_price", None))
                    if px is None or float(px or 0.0) <= 0:
                        px = self._safe_float(getattr(self, "latest_price", None))
                    if px is None or float(px or 0.0) <= 0:
                        return "immediate_trigger"
                    if s == "long":
                        retry_stop = min(float(sp), float(px) * 0.995)
                    else:
                        retry_stop = max(float(sp), float(px) * 1.005)
                    retry_stop = round(float(retry_stop), int(self.price_precision or 0))
                    if retry_stop <= 0:
                        return "immediate_trigger"
                    retry_params = dict(params or {})
                    retry_params["stopPrice"] = float(retry_stop)
                    self.exchange.create_order(self.ccxt_symbol, "STOP_MARKET", desired_o_side, float(q), None, retry_params)
                    self._refresh_open_orders_cache()
                    return "ok"
                except Exception:
                    return "immediate_trigger"
            if "4130" in msg:
                backoff_until = float(getattr(self, "_stop_backoff_until_ts", 0.0) or 0.0)
                if now < backoff_until:
                    return "skipped"
                setattr(self, "_stop_backoff_until_ts", now + 60.0)
                logger.info("止损单已存在(交易所限制同方向closePosition条件单唯一)，跳过更新")
                return "ok"
            if "4045" in msg:
                try:
                    self._raw_purge_stop_like_orders(
                        desired_side=("SELL" if desired_o_side == "sell" else "BUY"),
                        required_position_side=required_ps,
                        limit=300,
                    )
                    self._refresh_open_orders_cache()
                except Exception:
                    pass
                try:
                    self.exchange.create_order(self.ccxt_symbol, "STOP_MARKET", desired_o_side, float(q), None, params)
                    self._refresh_open_orders_cache()
                    return "ok"
                except Exception as e2:
                    msg2 = str(e2)
                    if ("-2021" in msg2) or ("immediately trigger" in msg2):
                        try:
                            px = None
                            if s == "long":
                                px = self._safe_float(getattr(self, "best_bid_price", None))
                            else:
                                px = self._safe_float(getattr(self, "best_ask_price", None))
                            if px is None or float(px or 0.0) <= 0:
                                px = self._safe_float(getattr(self, "latest_price", None))
                            if px is None or float(px or 0.0) <= 0:
                                return "immediate_trigger"
                            if s == "long":
                                retry_stop = min(float(sp), float(px) * 0.995)
                            else:
                                retry_stop = max(float(sp), float(px) * 1.005)
                            retry_stop = round(float(retry_stop), int(self.price_precision or 0))
                            if retry_stop <= 0:
                                return "immediate_trigger"
                            retry_params = dict(params or {})
                            retry_params["stopPrice"] = float(retry_stop)
                            self.exchange.create_order(self.ccxt_symbol, "STOP_MARKET", desired_o_side, float(q), None, retry_params)
                            self._refresh_open_orders_cache()
                            return "ok"
                        except Exception:
                            return "immediate_trigger"
                    logger.error(f"更新止损单失败: {e2}")
            else:
                logger.error(f"更新止损单失败: {e}")
        self._refresh_open_orders_cache()
        return "error"

    async def maybe_update_trailing_stop(self):
        if self.shutdown_event.is_set():
            return
        cfg = self.risk_engine.get_config()
        self._apply_runtime_settings_from_config()
        if not bool(cfg.get("TRAILING_STOP_ENABLED", False)):
            return
        if self.latest_price is None or float(self.latest_price or 0.0) <= 0:
            return
        side = str(self.direction or "long").strip().lower()
        if side not in {"long", "short"}:
            side = "long"
        try:
            snap = self.get_position_snapshot()
        except Exception:
            return
        amt = float(((snap.get(side) or {}).get("amt") or 0.0))
        entry = self._safe_float((snap.get(side) or {}).get("entry_price"))
        if amt <= 0 or entry is None or entry <= 0:
            if side == "long":
                self._trail_peak_price_long = None
                self._trail_anchor_entry_long = None
            else:
                self._trail_trough_price_short = None
                self._trail_anchor_entry_short = None
            async with self.lock:
                self._cancel_stop_orders_for_side(side)
            return
        stop_price = self._compute_trailing_stop_price(side, float(entry), float(self.latest_price), cfg)
        if stop_price is None:
            return
        px = self._safe_float(getattr(self, "best_bid_price" if side == "long" else "best_ask_price", None))
        if px is None or float(px or 0.0) <= 0:
            px = self._safe_float(getattr(self, "latest_price", None))
        if px is None:
            px = 0.0
        breached = (float(px) <= float(stop_price)) if side == "long" else (float(px) >= float(stop_price))
        if breached:
            pnl = float(self._safe_float((snap.get(side) or {}).get("pnl")) or 0.0)
            await self._trigger_hardstop(side, pnl, reason=f"trailing_stop_breached price={float(px):.6f} stop_price={float(stop_price):.6f}")
            return
        upsert_result = None
        async with self.lock:
            upsert_result = self._upsert_stop_market(side, float(amt), float(stop_price))
        if upsert_result == "immediate_trigger":
            pnl = float(self._safe_float((snap.get(side) or {}).get("pnl")) or 0.0)
            await self._trigger_hardstop(side, pnl, reason="stop_order_would_immediately_trigger")
            return

    async def maybe_trigger_hard_stoploss(self):
        if self.shutdown_event.is_set():
            return
        if bool(getattr(self, "is_grid_stopped", False)):
            return

        cfg = self.risk_engine.get_config()
        self._apply_runtime_settings_from_config()
        hs_price = self._safe_float(cfg.get("HARD_STOPLOSS_PRICE", 0.0))
        if hs_price is None or hs_price <= 0:
            return
        if float(self.latest_price or 0.0) <= 0:
            return

        transient_types = (
            getattr(ccxt, "NetworkError", Exception),
            getattr(ccxt, "RequestTimeout", Exception),
            getattr(ccxt, "ExchangeNotAvailable", Exception),
            getattr(ccxt, "DDoSProtection", Exception),
        )

        last_err = None
        long_amt = short_amt = long_pnl = short_pnl = None
        for attempt in range(2):
            try:
                long_amt, short_amt, long_pnl, short_pnl = self.get_position_risk()
                last_err = None
                break
            except Exception as e:
                last_err = e
                if attempt == 0 and isinstance(e, transient_types):
                    await asyncio.sleep(0.25)
                    continue
                break

        if last_err is not None:
            logger.error(f"获取持仓浮盈亏失败({type(last_err).__name__}): {last_err}")
            return
        self.long_position = float(long_amt or 0.0)
        self.short_position = float(short_amt or 0.0)

        active = str(self.direction or "long").strip().lower()
        if active not in {"long", "short"}:
            active = "long"
        if active == "long":
            amt = float(long_amt or 0.0)
            pnl = None if long_pnl is None else float(long_pnl)
        else:
            amt = float(short_amt or 0.0)
            pnl = None if short_pnl is None else float(short_pnl)

        if amt <= 0:
            return

        breached = False
        reason = None
        if active == "long" and float(self.latest_price) <= float(hs_price):
            breached = True
            reason = f"price={float(self.latest_price):.6f}<=stop_price={float(hs_price):.6f}"
        if active == "short" and float(self.latest_price) >= float(hs_price):
            breached = True
            reason = f"price={float(self.latest_price):.6f}>=stop_price={float(hs_price):.6f}"
        if not breached:
            return

        await self._trigger_hardstop(active, float(pnl or 0.0), str(reason or ""))

    def get_position_risk(self):
        params = {"type": "future"}
        positions = self.exchange.fetch_positions(params=params)
        long_amt = 0.0
        short_amt = 0.0
        long_pnl = 0.0
        short_pnl = 0.0

        for position in positions:
            if position.get("symbol") != self.ccxt_symbol:
                continue
            side = self._position_side_from_ccxt_position(position)
            contracts = self._position_amount_from_ccxt_position(position)
            pnl = self._safe_float(position.get("unrealizedPnl"))
            if pnl is None:
                pnl = self._safe_float(position.get("unrealizedProfit"))
            info = position.get("info") or {}
            if pnl is None:
                pnl = self._safe_float(info.get("unRealizedProfit"))
            if pnl is None:
                pnl = self._safe_float(info.get("unrealizedProfit"))
            if pnl is None:
                pnl = 0.0

            if side == "long":
                long_amt = max(0.0, float(contracts or 0.0))
                long_pnl = float(pnl)
            elif side == "short":
                short_amt = abs(float(contracts or 0.0))
                short_pnl = float(pnl)

        return float(long_amt), float(short_amt), float(long_pnl), float(short_pnl)

    async def _trigger_hardstop(self, loss_side: str, loss_pnl: float, reason: str = ""):
        if self.shutdown_event.is_set():
            return
        if bool(getattr(self, "is_grid_stopped", False)):
            return

        cfg = self.risk_engine.get_config()
        self.is_grid_stopped = True
        self._force_orders_resync = False

        msg = f"【硬止损触发】{loss_side} 单边浮亏 {loss_pnl:.6f}"
        if reason:
            msg = f"{msg} ({reason})"
        logger.error(msg)

        async with self.lock:
            try:
                self.cancel_orders_for_side(loss_side)
            except Exception:
                pass
            try:
                self._cancel_stop_orders_for_side(loss_side)
            except Exception:
                pass

            qty = float(self.long_position or 0.0) if loss_side == "long" else float(self.short_position or 0.0)
            if qty > 0:
                close_side = "sell" if loss_side == "long" else "buy"
                try:
                    self.place_order(
                        close_side,
                        price=None,
                        quantity=qty,
                        is_reduce_only=True,
                        position_side=loss_side,
                        order_type="market",
                    )
                except Exception:
                    pass

        if bool(cfg.get("STOP_ON_HARDSTOP", True)):
            try:
                await self.shutdown("hard_stoploss")
            except Exception:
                pass

    def _initialize_exchange(self):
        """初始化交易所 API"""
        exchange = CustomGate({
            "apiKey": self.api_key,
            "secret": self.api_secret,
            "timeout": 15000,
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",  # 使用永续合约
                "fetchCurrencies": False,
                "adjustForTimeDifference": True,
                "recvWindow": 10000,
            },
        })
        if self.account_mode in {"testnet", "paper", "sim"}:
            testnet_base = "https://testnet.binancefuture.com"
            try:
                api_urls = exchange.urls.get("api") or {}
                for k in list(api_urls.keys()):
                    if not str(k).lower().startswith("fapi"):
                        continue
                    v = api_urls.get(k)
                    if not v:
                        continue
                    s = str(v)
                    if "/fapi/" in s:
                        tail = s.split("/fapi/", 1)[1]
                        api_urls[k] = f"{testnet_base}/fapi/{tail}"
                    elif "/fapi" in s:
                        tail = s.split("/fapi", 1)[1]
                        api_urls[k] = f"{testnet_base}/fapi{tail}"
                    else:
                        api_urls[k] = f"{testnet_base}/fapi"
                exchange.urls["api"] = api_urls
            except Exception:
                pass
        try:
            exchange.load_time_difference()
        except Exception:
            pass
        # 加载市场数据
        exchange.load_markets(reload=False)
        return exchange

    def _get_price_precision(self):
        """获取交易对的价格精度、数量精度和最小下单数量"""
        markets = self.exchange.fetch_markets()
        symbol_info = next(market for market in markets if market["symbol"] == self.ccxt_symbol)

        # 获取价格精度
        price_precision = symbol_info["precision"]["price"]
        if isinstance(price_precision, float):
            # 如果 price_precision 是浮点数（例如 0.01），计算小数点后的位数
            self.price_precision = int(abs(math.log10(price_precision)))
        elif isinstance(price_precision, int):
            # 如果 price_precision 是整数，直接使用
            self.price_precision = price_precision
        else:
            raise ValueError(f"未知的价格精度类型: {price_precision}")

        # 获取数量精度
        amount_precision = symbol_info["precision"]["amount"]
        if isinstance(amount_precision, float):
            # 如果 amount_precision 是浮点数（例如 0.001），计算小数点后的位数
            self.amount_precision = int(abs(math.log10(amount_precision)))
        elif isinstance(amount_precision, int):
            # 如果 amount_precision 是整数，直接使用
            self.amount_precision = amount_precision
        else:
            raise ValueError(f"未知的数量精度类型: {amount_precision}")

        # 获取最小下单数量
        self.min_order_amount = symbol_info["limits"]["amount"]["min"]
        try:
            self.min_order_cost = (symbol_info.get("limits") or {}).get("cost", {}).get("min")
        except Exception:
            self.min_order_cost = None

        contract_size = symbol_info.get("contractSize")
        if contract_size is None:
            contract_size = (symbol_info.get("info") or {}).get("contractSize")
        try:
            self.contract_size = float(contract_size) if contract_size is not None else 1.0
        except Exception:
            self.contract_size = 1.0
        if float(self.contract_size or 0.0) <= 0:
            self.contract_size = 1.0

        logger.info(
            f"价格精度: {self.price_precision}, 数量精度: {self.amount_precision}, 最小下单数量: {self.min_order_amount}")

    def round_amount_up(self, amount: float):
        a = self._safe_float(amount)
        if a is None:
            return None
        p = int(self.amount_precision or 0)
        if p <= 0:
            return float(math.ceil(float(a)))
        factor = 10 ** p
        return float(math.ceil(float(a) * factor) / factor)

    def get_position(self):
        """获取当前持仓"""
        params = {
            'type': 'future'  # 永续合约
        }
        positions = self.exchange.fetch_positions(params=params)
        # print(positions)
        long_position = 0
        short_position = 0

        for position in positions:
            if position['symbol'] == self.ccxt_symbol:  # 使用动态的 symbol 变量
                side = self._position_side_from_ccxt_position(position)  # 获取仓位方向
                amount = abs(float(self._position_amount_from_ccxt_position(position) or 0.0))

                # 判断是否为多头或空头
                if side == 'long':  # 多头
                    long_position = amount
                elif side == 'short':  # 空头
                    short_position = amount  # 使用绝对值来计算空头合约数

        # 如果没有持仓，返回 0
        if long_position == 0 and short_position == 0:
            return 0, 0

        return long_position, short_position

    async def monitor_orders(self):
        """监控挂单状态，超过300秒未成交的挂单自动取消"""
        while True:
            try:
                await asyncio.sleep(60)  # 每60秒检查一次
                current_time = time.time()  # 当前时间（秒）
                orders = self.exchange.fetch_open_orders(self.ccxt_symbol)

                if not orders:
                    logger.info("当前没有未成交的挂单")
                    self.buy_long_orders = 0.0  # 多头买入剩余挂单数量
                    self.sell_long_orders = 0.0  # 多头卖出剩余挂单数量
                    self.sell_short_orders = 0.0  # 空头卖出剩余挂单数量
                    self.buy_short_orders = 0.0  # 空头买入剩余挂单数量
                    continue

                for order in orders:
                    order_id = order['id']
                    order_timestamp = order.get('timestamp')  # 获取订单创建时间戳（毫秒）
                    create_time = float(order['info'].get('create_time', 0))  # 获取订单创建时间（秒）

                    # 优先使用 create_time，如果不存在则使用 timestamp
                    order_time = create_time if create_time > 0 else order_timestamp / 1000

                    if not order_time:
                        logger.warning(f"订单 {order_id} 缺少时间戳，无法检查超时")
                        continue

                    if current_time - order_time > 300:  # 超过300秒未成交
                        logger.info(f"订单 {order_id} 超过300秒未成交，取消挂单")
                        try:
                            self.cancel_order(order_id)
                        except Exception as e:
                            logger.error(f"取消订单 {order_id} 失败: {e}")

            except Exception as e:
                logger.error(f"监控挂单状态失败: {e}")

    def check_orders_status(self):
        """检查当前所有挂单的状态，并更新多头和空头的挂单数量"""
        # 获取当前所有挂单（带 symbol 参数，限制为某个交易对）
        orders = self.exchange.fetch_open_orders(symbol=self.ccxt_symbol)

        # 初始化计数器
        buy_long_orders = 0.0  # 使用浮点数
        sell_long_orders = 0.0  # 使用浮点数
        buy_short_orders = 0.0  # 使用浮点数
        sell_short_orders = 0.0  # 使用浮点数

        for order in orders:
            try:
                if self._is_stop_order(order):
                    continue
                orig_quantity = self._safe_float((order.get('info') or {}).get('origQty'))
                if orig_quantity is None:
                    orig_quantity = self._safe_float(order.get("amount"))
                if orig_quantity is None:
                    continue
                orig_quantity = abs(float(orig_quantity))
                side = str(order.get('side') or '').strip().lower()
                if side not in {"buy", "sell"}:
                    continue
                reduce_only = self._get_order_reduce_only(order)

                if reduce_only:
                    if side == "sell":
                        sell_long_orders += orig_quantity
                    else:
                        buy_short_orders += orig_quantity
                else:
                    if side == "buy":
                        buy_long_orders += orig_quantity
                    else:
                        sell_short_orders += orig_quantity
            except Exception:
                continue

        # 更新实例变量
        self.buy_long_orders = buy_long_orders
        self.sell_long_orders = sell_long_orders
        self.buy_short_orders = buy_short_orders
        self.sell_short_orders = sell_short_orders

    def _fetch_last_price_rest(self):
        try:
            t = self.exchange.fetch_ticker(self.ccxt_symbol)
            p = self._safe_float(t.get("last"))
            if p is None:
                p = self._safe_float(t.get("close"))
            if p is not None and float(p) > 0:
                return float(p)
        except Exception:
            pass
        return None

    async def _maybe_open_base_position(self):
        cfg = {}
        try:
            cfg = self.risk_engine.get_config() or {}
        except Exception:
            cfg = {}
        if not bool(cfg.get("ENABLE_BASE_POSITION", False)):
            return
        base_usdc = self._safe_float(cfg.get("BASE_POSITION_USDC", 0.0))
        if base_usdc is None or float(base_usdc) <= 0:
            return
        if float(self.long_position or 0.0) > 0 or float(self.short_position or 0.0) > 0:
            return
        price = self._safe_float(getattr(self, "latest_price", None))
        if price is None or float(price) <= 0:
            price = self._fetch_last_price_rest()
        if price is None or float(price) <= 0:
            return
        qty = self.usdc_to_amount(float(base_usdc), float(price))
        if qty is None or float(qty) <= 0:
            return
        side = "buy" if str(self.direction or "long") == "long" else "sell"
        async with self.lock:
            self.place_order(
                side,
                price=None,
                quantity=float(qty),
                is_reduce_only=False,
                position_side=str(self.direction or "long"),
                order_type="market",
            )
        self._force_orders_resync = True

    async def run(self):
        """启动 WebSocket 监听"""
        self._apply_runtime_settings_from_config()
        asyncio.create_task(self.status_file_loop())
        # 初始化时获取一次持仓数据
        self.long_position, self.short_position = self.get_position()
        # self.last_position_update_time = time.time()
        logger.info(f"初始化持仓: 多头 {self.long_position} 张, 空头 {self.short_position} 张")

        # 等待状态同步完成
        await asyncio.sleep(float(self.order_first_time_sec or 0.0))

        # 初始化时获取一次挂单状态
        self.check_orders_status()
        logger.info(
            f"初始化挂单状态: 多头开仓={self.buy_long_orders}, 多头止盈={self.sell_long_orders}, 空头开仓={self.sell_short_orders}, 空头止盈={self.buy_short_orders}")

        await self._maybe_open_base_position()

        # 启动挂单监控任务
        # asyncio.create_task(self.monitor_orders())
        # 启动 listenKey 更新任务
        asyncio.create_task(self.keep_listen_key_alive())
        asyncio.create_task(self.status_log_loop())
        asyncio.create_task(self.risk_engine.config_watch_loop())

        while not self.shutdown_event.is_set():
            try:
                await self.connect_websocket()
            except Exception as e:
                if self.shutdown_event.is_set():
                    break
                logger.error(f"WebSocket 连接失败: {e}")
                await asyncio.sleep(5)  # 等待 5 秒后重试

    async def connect_websocket(self):
        """连接 WebSocket 并订阅 ticker 和持仓数据"""
        async with websockets.connect(self.websocket_url) as websocket:
            self._ws = websocket
            try:
                await self.subscribe_ticker(websocket)
                await self.subscribe_orders(websocket)
                while not self.shutdown_event.is_set():
                    try:
                        message = await websocket.recv()
                        self._last_ws_msg_ts = time.time()
                        data = json.loads(message)
                        if data.get("e") == "bookTicker":
                            await self.handle_ticker_update(message)
                        elif data.get("e") == "ORDER_TRADE_UPDATE":
                            await self.handle_order_update(message)
                    except Exception as e:
                        if self.shutdown_event.is_set():
                            break
                        logger.error(f"WebSocket 消息处理失败: {e}")
                        break
            finally:
                self._ws = None

    async def subscribe_ticker(self, websocket):
        """订阅 ticker 数据"""
        payload = {
            "method": "SUBSCRIBE",
            "params": [f"{self.coin_name.lower()}{self.contract_type.lower()}@bookTicker"],
            "id": 1
        }
        await websocket.send(json.dumps(payload))
        logger.info(f"已发送 ticker 订阅请求: {payload}")

    async def subscribe_orders(self, websocket):
        """订阅挂单数据"""
        if not self.listenKey:
            logger.error("listenKey 为空，无法订阅订单更新")
            return

        payload = {
            "method": "SUBSCRIBE",
            "params": [f"{self.listenKey}"],  # 使用 self.listenKey 订阅
            "id": 3
        }
        await websocket.send(json.dumps(payload))
        logger.info(f"已发送挂单订阅请求: {payload}")

    def get_listen_key(self):
        """获取 listenKey"""
        try:
            if not str(self.api_key or "").strip():
                raise ValueError("API_KEY 为空，无法获取 listenKey")
            if not str(self.api_secret or "").strip():
                raise ValueError("API_SECRET 为空，无法获取 listenKey")
            response = self.exchange.fapiPrivatePostListenKey()
            listenKey = response.get("listenKey")
            if not listenKey:
                raise ValueError("获取的 listenKey 为空")
            logger.info(f"成功获取 listenKey: {listenKey}")
            return listenKey
        except Exception as e:
            logger.error(f"获取 listenKey 失败: {e}")
            raise e

    async def keep_listen_key_alive(self):
        """定期更新 listenKey"""
        while not self.shutdown_event.is_set():
            try:
                await asyncio.sleep(1800)  # 每 30 分钟更新一次
                self.exchange.fapiPrivatePutListenKey()
                self.listenKey = self.get_listen_key()  # 更新 self.listenKey
                logger.info(f"listenKey 已更新: {self.listenKey}")
            except Exception as e:
                if self.shutdown_event.is_set():
                    break
                logger.error(f"更新 listenKey 失败: {e}")
                await asyncio.sleep(60)  # 等待 60 秒后重试

    def _generate_sign(self, message):
        """生成 HMAC-SHA256 签名"""
        return hmac.new(self.api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256).hexdigest()

    async def handle_ticker_update(self, message):
        current_time = time.time()
        if current_time - self.last_ticker_update_time < 0.5:  # 100ms
            return  # 跳过本次更新

        self.last_ticker_update_time = current_time
        """处理 ticker 更新"""
        data = json.loads(message)
        if data.get("e") == "bookTicker":  # Binance 的 bookTicker 事件
            best_bid_price = data.get("b")
            best_ask_price = data.get("a")

            # 校验字段是否存在且有效
            if best_bid_price is None or best_ask_price is None:
                logger.warning("bookTicker 消息中缺少最佳买价或最佳卖价")
                return

            try:
                self.best_bid_price = float(best_bid_price)  # 最佳买价
                self.best_ask_price = float(best_ask_price)  # 最佳卖价
                self.latest_price = (self.best_bid_price + self.best_ask_price) / 2  # 最新价格
                # logger.info(
                #     f"最新价格: {self.latest_price}, 最佳买价: {self.best_bid_price}, 最佳卖价: {self.best_ask_price}")
            except ValueError as e:
                logger.error(f"解析价格失败: {e}")

            # 检查持仓状态是否过时
            if time.time() - self.last_position_update_time > float(self.rest_sync_interval_sec or 0.0):
                self.long_position, self.short_position = self.get_position()
                self.last_position_update_time = time.time()
                logger.info(f"同步 position: 多头 {self.long_position} 张, 空头 {self.short_position} 张 @ ticker")

            # 检查持仓状态是否过时
            if time.time() - self.last_orders_update_time > float(self.rest_sync_interval_sec or 0.0):
                self.check_orders_status()
                self.last_orders_update_time = time.time()
                logger.info(f"同步 orders: 多头买单 {self.buy_long_orders} 张, 多头卖单 {self.sell_long_orders} 张,空头卖单 {self.sell_short_orders} 张, 空头买单 {self.buy_short_orders} 张 @ ticker")

            await self.maybe_update_trailing_stop()
            await self.maybe_trigger_take_profit()
            await self.maybe_trigger_hard_stoploss()
            await self.adjust_grid_strategy()

    async def handle_order_update(self, message):
        need_eval = False
        shutdown_reason = None
        async with self.lock:
            data = json.loads(message)
            if data.get("e") != "ORDER_TRADE_UPDATE":
                return

            order = data.get("o", {})
            symbol = order.get("s")
            if symbol != f"{self.coin_name}{self.contract_type}":
                return

            side = str(order.get("S") or "").strip().upper()
            position_side = str(order.get("ps") or "").strip().upper()
            status = order.get("X")
            quantity = float(order.get("q", 0))
            filled = float(order.get("z", 0))
            remaining = quantity - filled
            reduce_only = bool(order.get("R")) or bool(order.get("reduceOnly")) or bool(order.get("reduce_only"))
            exec_type = str(order.get("x") or order.get("X") or "").strip().upper()
            trade_id = order.get("t")
            if trade_id is None:
                trade_id = order.get("T")
            trade_sig = None
            try:
                if trade_id is not None:
                    trade_sig = f"{symbol}:{int(trade_id)}"
            except Exception:
                trade_sig = None

            if status == "NEW":
                if side == "BUY":
                    if reduce_only:
                        self.buy_short_orders += remaining
                    else:
                        self.buy_long_orders += remaining
                elif side == "SELL":
                    if reduce_only:
                        self.sell_long_orders += remaining
                    else:
                        self.sell_short_orders += remaining
            elif status == "FILLED":
                self.total_fills += 1
                if side == "BUY":
                    if reduce_only:
                        self.short_position = max(0.0, self.short_position - filled)
                        self.buy_short_orders = max(0.0, self.buy_short_orders - filled)
                    else:
                        self.buy_fills += 1
                        self.long_position += filled
                        self.buy_long_orders = max(0.0, self.buy_long_orders - filled)
                elif side == "SELL":
                    if reduce_only:
                        self.long_position = max(0.0, self.long_position - filled)
                        self.sell_long_orders = max(0.0, self.sell_long_orders - filled)
                    else:
                        self.sell_fills += 1
                        self.short_position += filled
                        self.sell_short_orders = max(0.0, self.sell_short_orders - filled)

                if reduce_only and _is_stop_like_ws_order(order):
                    r = _stop_reason_from_ws_order(order) or "hard_stoploss"
                    closed_side = "short" if side == "BUY" else "long"
                    if closed_side == "long" and float(self.long_position or 0.0) <= 0:
                        shutdown_reason = r
                    if closed_side == "short" and float(self.short_position or 0.0) <= 0:
                        shutdown_reason = r

                if exec_type == "TRADE":
                    if trade_sig is None or trade_sig not in self._last_trade_id_seen:
                        if trade_sig is not None:
                            self._last_trade_id_seen.add(trade_sig)
                            if len(self._last_trade_id_seen) > int(self._last_trade_id_seen_max):
                                self._last_trade_id_seen = set(list(self._last_trade_id_seen)[-int(self._last_trade_id_seen_max):])
                        rp = self._safe_float(order.get("rp"))
                        if rp is not None:
                            self.instance_realized_pnl += float(rp)
                        fee = self._safe_float(order.get("n"))
                        fee_asset = str(order.get("N") or "").strip().upper()
                        if fee is not None:
                            self.instance_fees_by_asset[fee_asset or "UNKNOWN"] = float(self.instance_fees_by_asset.get(fee_asset or "UNKNOWN", 0.0)) + float(fee)
                            if fee_asset == str(self.contract_type or "").strip().upper():
                                self.instance_fees += float(fee)
                fill_price = self._safe_float(order.get("ap"))
                if fill_price is None:
                    fill_price = self._safe_float(order.get("L"))
                anchor_ps = None
                if side == "BUY":
                    anchor_ps = "SHORT" if reduce_only else "LONG"
                elif side == "SELL":
                    anchor_ps = "LONG" if reduce_only else "SHORT"
                if anchor_ps is None:
                    if position_side in {"LONG", "SHORT"}:
                        anchor_ps = position_side
                    else:
                        anchor_ps = "LONG" if str(self.direction or "long") == "long" else "SHORT"
                self._update_anchor_after_fill(anchor_ps, float(fill_price or 0.0))
            elif status == "CANCELED":
                if side == "BUY":
                    if reduce_only:
                        self.buy_short_orders = max(0.0, self.buy_short_orders - quantity)
                    else:
                        self.buy_long_orders = max(0.0, self.buy_long_orders - quantity)
                elif side == "SELL":
                    if reduce_only:
                        self.sell_long_orders = max(0.0, self.sell_long_orders - quantity)
                    else:
                        self.sell_short_orders = max(0.0, self.sell_short_orders - quantity)

            if status in {"FILLED", "PARTIALLY_FILLED", "EXPIRED"}:
                if status == "PARTIALLY_FILLED":
                    if exec_type == "TRADE":
                        if trade_sig is None or trade_sig not in self._last_trade_id_seen:
                            if trade_sig is not None:
                                self._last_trade_id_seen.add(trade_sig)
                                if len(self._last_trade_id_seen) > int(self._last_trade_id_seen_max):
                                    self._last_trade_id_seen = set(list(self._last_trade_id_seen)[-int(self._last_trade_id_seen_max):])
                            rp = self._safe_float(order.get("rp"))
                            if rp is not None:
                                self.instance_realized_pnl += float(rp)
                            fee = self._safe_float(order.get("n"))
                            fee_asset = str(order.get("N") or "").strip().upper()
                            if fee is not None:
                                self.instance_fees_by_asset[fee_asset or "UNKNOWN"] = float(self.instance_fees_by_asset.get(fee_asset or "UNKNOWN", 0.0)) + float(fee)
                                if fee_asset == str(self.contract_type or "").strip().upper():
                                    self.instance_fees += float(fee)
                    fill_price = self._safe_float(order.get("ap"))
                    if fill_price is None:
                        fill_price = self._safe_float(order.get("L"))
                    anchor_ps = None
                    if side == "BUY":
                        anchor_ps = "SHORT" if reduce_only else "LONG"
                    elif side == "SELL":
                        anchor_ps = "LONG" if reduce_only else "SHORT"
                    if anchor_ps is None:
                        if position_side in {"LONG", "SHORT"}:
                            anchor_ps = position_side
                        else:
                            anchor_ps = "LONG" if str(self.direction or "long") == "long" else "SHORT"
                    self._update_anchor_after_fill(anchor_ps, float(fill_price or 0.0))
                need_eval = True

        if shutdown_reason and (not self.shutdown_event.is_set()):
            try:
                await self.shutdown(shutdown_reason)
            except Exception:
                pass
            return

        if need_eval:
            self._kick_risk_eval()

    def _kick_risk_eval(self):
        task = getattr(self, "_order_event_eval_task", None)
        if task is not None and (not task.done()):
            return
        if self.shutdown_event.is_set():
            return
        self._order_event_eval_task = asyncio.create_task(self._order_event_eval())

    async def _order_event_eval(self):
        try:
            await self.maybe_update_trailing_stop()
        except Exception:
            pass
        try:
            await self.maybe_trigger_take_profit()
        except Exception:
            pass
        try:
            await self.maybe_trigger_hard_stoploss()
        except Exception:
            pass
        try:
            await self.adjust_grid_strategy()
        except Exception:
            pass

    async def maybe_trigger_take_profit(self):
        if self.shutdown_event.is_set():
            return
        if bool(getattr(self, "is_grid_stopped", False)):
            return

        cfg = self.risk_engine.get_config()
        self._apply_runtime_settings_from_config()
        if not bool(cfg.get("TAKE_PROFIT_ENABLED", False)):
            return
        tp_price = self._safe_float(cfg.get("TAKE_PROFIT_PRICE", 0.0))
        if tp_price is None or float(tp_price) <= 0:
            return

        side = str(self.direction or "long").strip().lower()
        if side not in {"long", "short"}:
            side = "long"

        if side == "long":
            px = self._safe_float(getattr(self, "best_bid_price", None))
            if px is None or px <= 0:
                px = self._safe_float(getattr(self, "latest_price", None))
            if px is None or px <= 0 or float(px) < float(tp_price):
                return
        else:
            px = self._safe_float(getattr(self, "best_ask_price", None))
            if px is None or px <= 0:
                px = self._safe_float(getattr(self, "latest_price", None))
            if px is None or px <= 0 or float(px) > float(tp_price):
                return

        await self._trigger_take_profit(side, float(tp_price), float(px))

    async def _trigger_take_profit(self, side: str, tp_price: float, current_price: float):
        if self.shutdown_event.is_set():
            return
        if bool(getattr(self, "is_grid_stopped", False)):
            return

        self.is_grid_stopped = True
        self._force_orders_resync = False

        logger.info(f"【自定义止盈触发】side={side} price={current_price:.6f} tp_price={tp_price:.6f}")

        async with self.lock:
            try:
                self.cancel_all_open_orders()
            except Exception:
                pass
            try:
                self._cancel_stop_orders_for_side("long")
            except Exception:
                pass
            try:
                self._cancel_stop_orders_for_side("short")
            except Exception:
                pass
            try:
                self.flatten_all_positions_market()
            except Exception:
                pass

        try:
            await self.shutdown("take_profit")
        except Exception:
            pass

    def cancel_orders_for_side(self, position_side):
        """撤销某个方向的所有挂单"""
        orders = self._get_open_orders_cached(max_age_sec=0.0)

        if len(orders) == 0:
            logger.info("没有找到挂单")
        else:
            try:
                for order in orders:
                    try:
                        if self._is_stop_order(order):
                            continue
                        side = str(order.get('side') or '').strip().lower()
                        if side not in {"buy", "sell"}:
                            continue
                        reduce_only = self._get_order_reduce_only(order)
                        ps = self._get_order_position_side(order)
                        if bool(getattr(self, "_hedge_mode", False)):
                            if position_side == "long" and ps not in {"LONG"}:
                                continue
                            if position_side == "short" and ps not in {"SHORT"}:
                                continue

                        if position_side == 'long':
                            if (not reduce_only) and side == 'buy':
                                self.cancel_order(order['id'])
                            elif reduce_only and side == 'sell':
                                self.cancel_order(order['id'])
                        elif position_side == 'short':
                            if (not reduce_only) and side == 'sell':
                                self.cancel_order(order['id'])
                            elif reduce_only and side == 'buy':
                                self.cancel_order(order['id'])
                    except Exception:
                        continue
            except ccxt.OrderNotFound as e:
                logger.warning(f"订单 {order['id']} 不存在，无需撤销: {e}")
                self.check_orders_status()  # 强制更新挂单状态
            except Exception as e:
                logger.error(f"撤单失败: {e}")
            finally:
                self._refresh_open_orders_cache()

    def cancel_order(self, order_id):
        """撤单"""
        try:
            self.exchange.cancel_order(order_id, self.ccxt_symbol)
            # logger.info(f"撤销挂单成功, 订单ID: {order_id}")
        except ccxt.BaseError as e:
            logger.error(f"撤单失败: {e}")

    def place_order(self, side, price, quantity, is_reduce_only=False, position_side=None, order_type='limit', client_order_id=None):
        """挂单函数，增加双向持仓支持"""
        try:
            if quantity is None:
                return None
            price = round(price, self.price_precision) if price is not None else None

            # 修正数量精度并确保不低于最小下单数量
            quantity = round(quantity, self.amount_precision)
            if float(quantity or 0.0) <= 0:
                return None
            min_amt = float(self.min_order_amount or 0.0)
            if min_amt > 0 and float(quantity) < min_amt:
                return None

            # 如果是市价单，不需要价格参数
            if order_type == 'market':
                params = {
                    'newClientOrderId': str(client_order_id or uuid.uuid4()),
                }
                if bool(is_reduce_only) and (not bool(getattr(self, "_hedge_mode", False))):
                    params["reduceOnly"] = True
                if bool(getattr(self, "_hedge_mode", False)) and position_side is not None:
                    params['positionSide'] = str(position_side).strip().upper()
                order = self.exchange.create_order(self.ccxt_symbol, 'market', side, quantity, None, params)
                return order
            else:
                # 检查 price 是否为 None
                if price is None:
                    logger.error("限价单必须提供 price 参数")
                    return None

                maker_only = self._maker_only_enabled()
                if maker_only:
                    price_po = self._post_only_price(side, price)
                    if price_po is None:
                        return None
                    price = price_po

                if (not bool(is_reduce_only)) and position_side is not None:
                    desired_ps = str(position_side).strip().upper()
                    if desired_ps in {"LONG", "SHORT"}:
                        pass
                    else:
                        desired_ps = str(position_side).strip().lower()
                        if desired_ps in {"long", "short"}:
                            desired_ps = desired_ps.upper()
                        else:
                            desired_ps = None
                    if desired_ps in {"LONG", "SHORT"} and self._has_duplicate_add_order(side, price, desired_ps):
                        logger.info(
                            f"已存在相同方向/仓位方向/价格的补仓单，跳过挂单: {side} {desired_ps} @ {price}"
                        )
                        return None

                params = {
                    'newClientOrderId': str(client_order_id or uuid.uuid4()),
                }
                if bool(is_reduce_only) and (not bool(getattr(self, "_hedge_mode", False))):
                    params["reduceOnly"] = True
                if bool(getattr(self, "_hedge_mode", False)) and position_side is not None:
                    params['positionSide'] = str(position_side).strip().upper()
                if maker_only:
                    params["timeInForce"] = "GTX"
                try:
                    order = self.exchange.create_order(self.ccxt_symbol, 'limit', side, quantity, price, params)
                    return order
                except ccxt.BaseError as e:
                    if maker_only:
                        msg = str(e).lower()
                        if ("5022" in msg) or ("post" in msg and "only" in msg) or ("immediately match" in msg):
                            ps = str(position_side or "").strip().lower()
                            if ps in {"long", "short"}:
                                self._mark_postonly_reject(ps)
                            return None
                    raise e

        except ccxt.BaseError as e:
            logger.error(f"下单报错: {e}")
            return None

    # ==================== 策略逻辑 ====================
    async def adjust_grid_strategy(self):

        if self.shutdown_event.is_set():
            return
        if bool(getattr(self, "is_grid_stopped", False)):
            return
        if self.latest_price is None or float(self.latest_price or 0.0) <= 0:
            return

        cfg = self.risk_engine.get_config()
        self._apply_runtime_settings_from_config()
        maker_only = self._maker_only_enabled()
        min_interval = float(cfg.get("RISK_EVAL_MIN_INTERVAL_SEC", 0.8))
        now = time.time()
        if min_interval > 0 and (now - float(getattr(self, "_last_risk_eval_ts", 0.0) or 0.0)) < min_interval:
            return
        self._last_risk_eval_ts = now

        async with self.lock:
            if bool(getattr(self, "is_grid_stopped", False)):
                return

            try:
                orders = self._get_open_orders_cached(max_age_sec=0.5) or []
            except Exception:
                orders = []

            long_pos = float(self.long_position or 0.0)
            short_pos = float(self.short_position or 0.0)

            active_side = str(self.direction or "long").strip().lower()
            if active_side not in {"long", "short"}:
                active_side = "long"
            active_sides = (active_side,)

            plans = {}
            for s in active_sides:
                p = long_pos if s == "long" else short_pos
                plans[s] = self.risk_engine.side_plan(s, float(self.latest_price), float(p))

            for side in active_sides:
                pos = long_pos if side == "long" else short_pos
                plan = plans.get(side) or {}
                if not bool(plan.get("enabled", False)):
                    continue

                if pos <= 0:
                    if side == "long":
                        self.mid_price_long = 0.0
                    else:
                        self.mid_price_short = 0.0

                if pos > 0:
                    if side == "long":
                        anchor = float(self.mid_price_long or 0.0)
                        if anchor <= 0:
                            self.mid_price_long = float(self.latest_price)
                            anchor = float(self.mid_price_long)
                    else:
                        anchor = float(self.mid_price_short or 0.0)
                        if anchor <= 0:
                            self.mid_price_short = float(self.latest_price)
                            anchor = float(self.mid_price_short)

                    add_spacing = float((plan.get("add") or {}).get("spacing") or 0.0)
                    tp_spacing = float((plan.get("tp") or {}).get("spacing") or 0.0)
                    add_usdc = float((plan.get("add") or {}).get("size_usdc") or 0.0)
                    tp_usdc = float((plan.get("tp") or {}).get("size_usdc") or 0.0)

                    if side == "long":
                        fixed_add_price = anchor * (1.0 - add_spacing)
                        fixed_tp_price = anchor * (1.0 + tp_spacing)
                    else:
                        fixed_add_price = anchor * (1.0 + add_spacing)
                        fixed_tp_price = anchor * (1.0 - tp_spacing)

                    if fixed_add_price > 0:
                        (plan.get("add") or {})["price"] = float(fixed_add_price)
                        if add_usdc > 0:
                            (plan.get("add") or {})["qty"] = self.usdc_to_amount(add_usdc, fixed_add_price)
                    if fixed_tp_price > 0:
                        (plan.get("tp") or {})["price"] = float(fixed_tp_price)
                        if tp_usdc > 0:
                            tp_qty_calc = self.usdc_to_amount(tp_usdc, fixed_tp_price)
                            if tp_qty_calc is not None:
                                tp_qty_calc = min(float(tp_qty_calc), float(pos))
                                tp_qty_calc = self.round_amount_down(tp_qty_calc)
                                if tp_qty_calc is not None:
                                    tp_qty_calc = min(float(tp_qty_calc), float(pos))
                                if tp_qty_calc is not None and float(tp_qty_calc) >= float(self.min_order_amount or 0.0):
                                    (plan.get("tp") or {})["qty"] = tp_qty_calc

                desired_add_id = (plan.get("add") or {}).get("client_id")
                desired_tp_id = (plan.get("tp") or {}).get("client_id")
                desired_add_id = str(desired_add_id) if desired_add_id else None
                desired_tp_id = str(desired_tp_id) if desired_tp_id else None

                add_present = False
                tp_present = False
                add_order_ts = None
                add_ok = False
                tp_ok = False
                add_id_ok = False

                add_price_target = self._safe_float((plan.get("add") or {}).get("price")) or 0.0
                tp_price_target = self._safe_float((plan.get("tp") or {}).get("price")) or 0.0
                add_price_target = round(float(add_price_target), int(self.price_precision or 0)) if add_price_target > 0 else 0.0
                tp_price_target = round(float(tp_price_target), int(self.price_precision or 0)) if tp_price_target > 0 else 0.0
                add_side = "buy" if side == "long" else "sell"
                tp_side = "sell" if side == "long" else "buy"
                required_ps = None
                if bool(getattr(self, "_hedge_mode", False)):
                    required_ps = "LONG" if side == "long" else "SHORT"

                for o in orders:
                    if self._is_stop_order(o):
                        continue
                    ps = self._get_order_position_side(o)
                    ro = self._get_order_reduce_only(o)
                    o_side = str((o or {}).get("side") or "").strip().lower()
                    if o_side not in {"buy", "sell"}:
                        continue
                    o_qty = self._safe_float((o or {}).get("remaining"))
                    if o_qty is None:
                        o_qty = self._safe_float((o or {}).get("amount"))
                    if o_qty is None:
                        o_qty = self._safe_float(((o or {}).get("info") or {}).get("origQty"))
                    if o_qty is None or float(o_qty) <= 0:
                        continue
                    o_price = self._safe_float((o or {}).get("price"))
                    if o_price is None:
                        o_price = self._safe_float(((o or {}).get("info") or {}).get("price"))
                    if o_price is None or float(o_price) <= 0:
                        continue
                    o_price = round(float(o_price), int(self.price_precision or 0))
                    o_cid = self._get_order_client_id(o)
                    if required_ps is not None and ps != required_ps:
                        continue
                    if side == "long":
                        if (not ro) and o_side == add_side:
                            add_present = True
                            add_order_ts = self._get_order_timestamp_sec(o) or add_order_ts
                            if desired_add_id is not None and str(o_cid or "") == desired_add_id:
                                add_id_ok = True
                            if o_price == add_price_target:
                                if desired_add_id is None or str(o_cid or "") == desired_add_id:
                                    add_ok = True
                        if ro and o_side == tp_side:
                            tp_present = True
                            if o_price == tp_price_target:
                                if desired_tp_id is None or str(o_cid or "") == desired_tp_id:
                                    tp_ok = True
                    else:
                        if (not ro) and o_side == add_side:
                            add_present = True
                            add_order_ts = self._get_order_timestamp_sec(o) or add_order_ts
                            if desired_add_id is not None and str(o_cid or "") == desired_add_id:
                                add_id_ok = True
                            if o_price == add_price_target:
                                if desired_add_id is None or str(o_cid or "") == desired_add_id:
                                    add_ok = True
                        if ro and o_side == tp_side:
                            tp_present = True
                            if o_price == tp_price_target:
                                if desired_tp_id is None or str(o_cid or "") == desired_tp_id:
                                    tp_ok = True

                add_qty = (plan.get("add") or {}).get("qty")
                tp_qty = (plan.get("tp") or {}).get("qty")
                need_tp = pos > 0 and tp_qty is not None and float(tp_qty) > 0
                if not need_tp:
                    tp_ok = True

                refresh_initial = False
                first_wait = self._safe_float(cfg.get("ORDER_FIRST_TIME_SEC", self.order_first_time_sec))
                if first_wait is None:
                    first_wait = float(self.order_first_time_sec or 0.0)
                if pos <= 0 and add_present and float(first_wait or 0.0) > 0:
                    last_ts = float(self.last_long_order_time or 0.0) if side == "long" else float(self.last_short_order_time or 0.0)
                    if last_ts <= 0:
                        if side == "long":
                            self.last_long_order_time = now
                        else:
                            self.last_short_order_time = now
                        last_ts = now
                    base_ts = float(add_order_ts or 0.0) if add_order_ts is not None else last_ts
                    if base_ts <= 0:
                        base_ts = last_ts
                    if (now - float(base_ts)) >= float(first_wait or 0.0):
                        refresh_initial = True

                if pos > 0:
                    need_reset = bool(getattr(self, "_force_orders_resync", False)) or (not add_ok) or (not tp_ok)
                else:
                    need_reset = (
                        bool(getattr(self, "_force_orders_resync", False))
                        or (not add_present)
                        or (desired_add_id is not None and (not add_id_ok))
                        or refresh_initial
                    )

                if need_reset and (not self._in_grid_action_cooldown(side)):
                    if maker_only and self._recent_postonly_reject(side):
                        continue
                    self._mark_grid_action(side)
                    try:
                        self.cancel_orders_for_side(side)
                    except Exception:
                        pass

                    if pos <= 0 and refresh_initial:
                        logger.info(f"刷新 {side} 初始开仓挂单到最优价")

                    if add_qty is not None and float(add_qty) > 0:
                        if pos <= 0:
                            best = self.best_bid_price if side == "long" else self.best_ask_price
                            add_price = float(best or 0.0)
                            if add_price <= 0:
                                add_price = float((plan.get("add") or {}).get("price") or 0.0)
                            add_usdc = float((plan.get("add") or {}).get("size_usdc") or 0.0)
                            if add_usdc > 0 and add_price > 0:
                                add_qty = self.usdc_to_amount(add_usdc, add_price)
                        else:
                            add_price = float((plan.get("add") or {}).get("price") or 0.0)

                        o = self.place_order(
                            add_side,
                            price=add_price,
                            quantity=float(add_qty),
                            is_reduce_only=False,
                            position_side=side,
                            order_type="limit",
                            client_order_id=desired_add_id,
                        )
                        if pos <= 0 and o is not None:
                            if side == "long":
                                self.last_long_order_time = now
                            else:
                                self.last_short_order_time = now

                    if need_tp:
                        tp_price = float((plan.get("tp") or {}).get("price") or 0.0)
                        tp_side = "sell" if side == "long" else "buy"
                        self.place_order(
                            tp_side,
                            price=tp_price,
                            quantity=float(tp_qty),
                            is_reduce_only=True,
                            position_side=side,
                            order_type="limit",
                            client_order_id=desired_tp_id,
                        )

            self._force_orders_resync = False

    def cancel_all_open_orders(self):
        try:
            self._raw_cancel_all_open_orders_for_symbol()
        except Exception:
            pass
        orders = self.exchange.fetch_open_orders(symbol=self.ccxt_symbol)
        for order in orders:
            oid = order.get("id")
            if not oid:
                continue
            try:
                self.cancel_order(oid)
            except Exception:
                pass
        try:
            self._raw_cancel_all_open_orders_for_symbol()
        except Exception:
            pass

    def flatten_all_positions_market(self):
        long_pos, short_pos = self.get_position()
        if long_pos and long_pos > 0:
            try:
                self.place_order(
                    "sell",
                    price=None,
                    quantity=float(long_pos),
                    is_reduce_only=True,
                    position_side="long",
                    order_type="market",
                )
            except Exception:
                pass
        if short_pos and short_pos > 0:
            try:
                self.place_order(
                    "buy",
                    price=None,
                    quantity=float(short_pos),
                    is_reduce_only=True,
                    position_side="short",
                    order_type="market",
                )
            except Exception:
                pass

    async def shutdown(self, reason: str):
        if getattr(self, "_shutting_down", False):
            self.shutdown_event.set()
            return
        self._shutting_down = True
        self._shutdown_reason = str(reason or "").strip()
        self.shutdown_event.set()
        logger.info(f"开始优雅退出: {reason}")
        try:
            s, msg = _ui_state_for_shutdown_reason(reason)
            if msg:
                self._set_last_error(msg)
        except Exception:
            pass
        try:
            r = str(reason or "").strip().lower()
            if ("hard_stop" in r) or ("take_profit" in r) or ("stoploss" in r) or ("stop_loss" in r):
                _disable_instance_autostart(self.strategy_config_path, self._status_dir, self.instance_id)
        except Exception:
            pass
        try:
            self._write_status_file()
        except Exception:
            pass
        try:
            os.remove(self._stop_flag_path)
        except Exception:
            pass
        try:
            if self._ws is not None:
                await self._ws.close()
        except Exception:
            pass
        try:
            self.cancel_all_open_orders()
        except Exception:
            pass
        try:
            self.flatten_all_positions_market()
        except Exception:
            pass
        try:
            self.cancel_all_open_orders()
        except Exception:
            pass
        logger.info(f"已执行优雅退出: {reason}")


# ==================== 主程序 ====================
if __name__ == "__main__":
    bot = None
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        strategy_config_path = os.getenv("STRATEGY_CONFIG_PATH", os.path.join(_script_dir, "config.json"))
        account_mode = _get_account_mode(strategy_config_path)
        params = _get_strategy_params(strategy_config_path)
        if account_mode in {"testnet", "paper", "sim"}:
            bot = GridTradingBot(
                TESTNET_API_KEY,
                TESTNET_API_SECRET,
                params["coin_name"],
                params["contract_type"],
                GRID_SPACING,
                INITIAL_QUANTITY,
                LEVERAGE,
                account_mode,
                params["rest_sync_interval_sec"],
                params["order_first_time_sec"],
            )
        else:
            bot = GridTradingBot(
                API_KEY,
                API_SECRET,
                params["coin_name"],
                params["contract_type"],
                GRID_SPACING,
                INITIAL_QUANTITY,
                LEVERAGE,
                account_mode,
                params["rest_sync_interval_sec"],
                params["order_first_time_sec"],
            )

        def _handle(sig, _frame=None):
            if bot is None:
                return
            if bot.shutdown_event.is_set():
                return
            sig_name = None
            try:
                sig_name = getattr(sig, "name", None)
            except Exception:
                sig_name = None
            if not sig_name:
                try:
                    sig_name = signal.Signals(int(sig)).name
                except Exception:
                    sig_name = str(sig)
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(bot.shutdown(f"signal_{sig_name}"))
            )

        for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None), getattr(signal, "SIGBREAK", None)):
            if sig is None:
                continue
            try:
                signal.signal(sig, _handle)
            except Exception:
                pass

        loop.run_until_complete(bot.run())
    except KeyboardInterrupt:
        if bot is not None:
            try:
                loop.run_until_complete(bot.shutdown("keyboard_interrupt"))
            except Exception:
                pass
    finally:
        try:
            pending = asyncio.all_tasks(loop)
            for t in pending:
                t.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        except Exception:
            pass
        try:
            loop.close()
        except Exception:
            pass
