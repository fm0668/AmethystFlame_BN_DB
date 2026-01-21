import asyncio
import hashlib
import json
import os
import time


class RiskEngine:
    def __init__(self, bot, config_path: str):
        self.bot = bot
        self.config_path = config_path
        self._config = None
        self._config_mtime = None
        self._config_version = 0
        self._last_config_error_ts = 0.0
        self._last_config_error_sig = None
        self._last_long_stage = 0
        self._last_short_stage = 0
        self._last_long_stage_change_ts = 0.0
        self._last_short_stage_change_ts = 0.0
        self._stage1_down_confirm = 0
        self._stage2_down_confirm = 0
        self._stage1_up_confirm = 0
        self._stage2_up_confirm = 0

    def get_config(self) -> dict:
        cfg = self._config
        if isinstance(cfg, dict):
            return cfg
        return self.default_config()

    def default_config(self) -> dict:
        return {
            "MAKER_ONLY": False,
            "DIRECTION": "long",
            "ALLOCATED_CAPITAL_USDC": 0.0,
            "ENABLE_BASE_POSITION": False,
            "BASE_POSITION_USDC": 0.0,
            "BASE_GRID_SPACING": 0.0025,
            "BASE_ORDER_SIZE_USDC": 40.0,
            "STAGE1_THRESHOLD_USDC": 400.0,
            "STAGE1_EXIT_USDC": 380.0,
            "STAGE1_TP_SPACING": 0.0012,
            "STAGE1_TP_SIZE_USDC": 80.0,
            "STAGE2_THRESHOLD_USDC": 600.0,
            "STAGE2_EXIT_USDC": 560.0,
            "STAGE2_ADD_SPACING": 0.0050,
            "STAGE2_ADD_SIZE_USDC": 25.0,
            "STAGE2_OPP_TP_SIZE_USDC": 0.0,
            "STAGE2_OPP_ADD_SIZE_USDC": 0.0,
            "REST_SYNC_INTERVAL_SEC": 10.0,
            "ORDER_FIRST_TIME_SEC": 10.0,
            "GRID_ACTION_COOLDOWN_SEC": 1.2,
            "STATUS_LOG_INTERVAL_SEC": 60.0,
            "RISK_EVAL_MIN_INTERVAL_SEC": 0.8,
            "STAGE_SWITCH_COOLDOWN_SEC": 15.0,
            "STAGE_SWITCH_CONFIRM_COUNT": 2,
            "HARD_STOPLOSS_PNL_USDC": 20.0,
            "HARD_STOPLOSS_CONFIRM_COUNT": 2,
            "HARD_STOPLOSS_CHECK_INTERVAL_SEC": 1.0,
            "STOP_ON_HARDSTOP": True,
            "HARD_STOPLOSS_PRICE": 0.0,
            "TAKE_PROFIT_ENABLED": False,
            "TAKE_PROFIT_PRICE": 0.0,
            "TRAILING_STOP_ENABLED": False,
            "TRAILING_STOP_BASE_STOP_RATIO": 0.0,
            "TRAILING_STOP_LADDER": [],
            "TRAILING_PULLBACK_LADDER": [],
            "ORDER_CLIENT_ID_PREFIX": "AF",
            "HOT_RELOAD_ENABLED": True,
            "CONFIG_WATCH_INTERVAL_SEC": 1.0,
            "CONFIG_ERROR_LOG_INTERVAL_SEC": 10.0,
        }

    def _normalize_raw_config(self, raw: dict) -> dict:
        mapping = {
            "交易方向": "DIRECTION",
            "方向": "DIRECTION",
            "分配资金": "ALLOCATED_CAPITAL_USDC",
            "allocated_capital": "ALLOCATED_CAPITAL_USDC",
            "allocated_capital_usdc": "ALLOCATED_CAPITAL_USDC",
            "allocated_capital_usdt": "ALLOCATED_CAPITAL_USDC",
            "启用底仓": "ENABLE_BASE_POSITION",
            "底仓金额": "BASE_POSITION_USDC",
            "只做MAKER": "MAKER_ONLY",
            "只做Maker": "MAKER_ONLY",
            "基础网格间距": "BASE_GRID_SPACING",
            "基础下单金额USDC": "BASE_ORDER_SIZE_USDC",
            "基础下单金额USDT": "BASE_ORDER_SIZE_USDC",
            "阶段1阈值USDC": "STAGE1_THRESHOLD_USDC",
            "阶段1阈值USDT": "STAGE1_THRESHOLD_USDC",
            "阶段1退出阈值USDC": "STAGE1_EXIT_USDC",
            "阶段1退出阈值USDT": "STAGE1_EXIT_USDC",
            "阶段1止盈间距": "STAGE1_TP_SPACING",
            "阶段1止盈金额USDC": "STAGE1_TP_SIZE_USDC",
            "阶段1止盈金额USDT": "STAGE1_TP_SIZE_USDC",
            "阶段2阈值USDC": "STAGE2_THRESHOLD_USDC",
            "阶段2阈值USDT": "STAGE2_THRESHOLD_USDC",
            "阶段2退出阈值USDC": "STAGE2_EXIT_USDC",
            "阶段2退出阈值USDT": "STAGE2_EXIT_USDC",
            "阶段2补仓间距": "STAGE2_ADD_SPACING",
            "阶段2补仓金额USDC": "STAGE2_ADD_SIZE_USDC",
            "阶段2补仓金额USDT": "STAGE2_ADD_SIZE_USDC",
            "阶段2对手方止盈金额USDC": "STAGE2_OPP_TP_SIZE_USDC",
            "阶段2对手方补仓金额USDC": "STAGE2_OPP_ADD_SIZE_USDC",
            "状态同步间隔秒": "REST_SYNC_INTERVAL_SEC",
            "首次下单等待秒": "ORDER_FIRST_TIME_SEC",
            "最小重挂间隔秒": "GRID_ACTION_COOLDOWN_SEC",
            "状态日志间隔秒": "STATUS_LOG_INTERVAL_SEC",
            "风控最小评估间隔秒": "RISK_EVAL_MIN_INTERVAL_SEC",
            "阶段切换冷却秒": "STAGE_SWITCH_COOLDOWN_SEC",
            "阶段切换确认次数": "STAGE_SWITCH_CONFIRM_COUNT",
            "单边浮亏硬止损USDC": "HARD_STOPLOSS_PNL_USDC",
            "单边浮亏硬止损USDT": "HARD_STOPLOSS_PNL_USDC",
            "硬止损确认次数": "HARD_STOPLOSS_CONFIRM_COUNT",
            "硬止损检查间隔秒": "HARD_STOPLOSS_CHECK_INTERVAL_SEC",
            "硬止损后停止策略": "STOP_ON_HARDSTOP",
            "硬止损价格": "HARD_STOPLOSS_PRICE",
            "止盈启用": "TAKE_PROFIT_ENABLED",
            "止盈价格": "TAKE_PROFIT_PRICE",
            "启用移动止损": "TRAILING_STOP_ENABLED",
            "移动止损初始止损比例": "TRAILING_STOP_BASE_STOP_RATIO",
            "移动止损阶梯": "TRAILING_STOP_LADDER",
            "移动止损回撤阶梯": "TRAILING_PULLBACK_LADDER",
            "订单ID前缀": "ORDER_CLIENT_ID_PREFIX",
            "启用热加载": "HOT_RELOAD_ENABLED",
            "热加载检查间隔秒": "CONFIG_WATCH_INTERVAL_SEC",
            "热加载错误日志间隔秒": "CONFIG_ERROR_LOG_INTERVAL_SEC",
        }
        out = {}
        for k, v in (raw or {}).items():
            out[mapping.get(k, k)] = v
        return out
    
    def _extract_nested_config(self, raw: dict) -> dict:
        out = {}
        if not isinstance(raw, dict):
            return out

        grid = raw.get("网格")
        if isinstance(grid, dict):
            if "方向" in grid:
                out["DIRECTION"] = grid.get("方向")
            if "间距比例" in grid:
                out["BASE_GRID_SPACING"] = grid.get("间距比例")
            if "每格金额" in grid:
                out["BASE_ORDER_SIZE_USDC"] = grid.get("每格金额")
            if "只做MAKER" in grid:
                out["MAKER_ONLY"] = grid.get("只做MAKER")
            if "只做Maker" in grid:
                out["MAKER_ONLY"] = grid.get("只做Maker")

        funds = raw.get("资金")
        if isinstance(funds, dict):
            if "分配资金" in funds:
                out["ALLOCATED_CAPITAL_USDC"] = funds.get("分配资金")

        base = raw.get("底仓")
        if isinstance(base, dict):
            if "启用" in base:
                out["ENABLE_BASE_POSITION"] = base.get("启用")
            if "金额" in base:
                out["BASE_POSITION_USDC"] = base.get("金额")

        sync = raw.get("同步")
        if isinstance(sync, dict):
            if "状态同步间隔秒" in sync:
                out["REST_SYNC_INTERVAL_SEC"] = sync.get("状态同步间隔秒")
            if "最小重挂间隔秒" in sync:
                out["GRID_ACTION_COOLDOWN_SEC"] = sync.get("最小重挂间隔秒")
            if "状态日志间隔秒" in sync:
                out["STATUS_LOG_INTERVAL_SEC"] = sync.get("状态日志间隔秒")

        hot = raw.get("热加载")
        if isinstance(hot, dict):
            if "启用" in hot:
                out["HOT_RELOAD_ENABLED"] = hot.get("启用")
            if "检查间隔秒" in hot:
                out["CONFIG_WATCH_INTERVAL_SEC"] = hot.get("检查间隔秒")

        stop = raw.get("硬止损")
        if isinstance(stop, dict):
            if "价格" in stop:
                out["HARD_STOPLOSS_PRICE"] = stop.get("价格")

        tp = raw.get("止盈")
        if isinstance(tp, dict):
            if "启用" in tp:
                out["TAKE_PROFIT_ENABLED"] = tp.get("启用")
            if "价格" in tp:
                out["TAKE_PROFIT_PRICE"] = tp.get("价格")

        trailing = raw.get("移动硬止损")
        if not isinstance(trailing, dict):
            trailing = raw.get("移动止损")
        if isinstance(trailing, dict):
            if "启用" in trailing:
                out["TRAILING_STOP_ENABLED"] = trailing.get("启用")
            if "初始止损比例" in trailing:
                out["TRAILING_STOP_BASE_STOP_RATIO"] = trailing.get("初始止损比例")
            if "阶梯" in trailing:
                out["TRAILING_STOP_LADDER"] = trailing.get("阶梯")
            if "回撤阶梯" in trailing:
                out["TRAILING_PULLBACK_LADDER"] = trailing.get("回撤阶梯")

        account_mode = raw.get("账户模式") or raw.get("交易环境") or raw.get("ACCOUNT_MODE") or raw.get("account_mode") or raw.get("环境")
        if account_mode is not None and "账户模式" in raw:
            out["账户模式"] = account_mode

        pair = raw.get("交易对")
        if isinstance(pair, dict):
            if "币" in pair:
                out["交易币种"] = pair.get("币")
            if "计价" in pair:
                out["合约类型"] = pair.get("计价")

        return out

    def _validate(self, cfg: dict) -> dict:
        direction = str(cfg.get("DIRECTION", "long")).strip().lower()
        if direction in {"做多", "多", "long", "l", "buy"}:
            direction = "long"
        elif direction in {"做空", "空", "short", "s", "sell"}:
            direction = "short"
        else:
            raise ValueError("DIRECTION must be long/short")
        cfg["DIRECTION"] = direction

        allocated = float(cfg.get("ALLOCATED_CAPITAL_USDC", cfg.get("ALLOCATED_CAPITAL_USDT", 0.0)) or 0.0)
        if allocated < 0:
            raise ValueError("ALLOCATED_CAPITAL_USDC must be >= 0")
        cfg["ALLOCATED_CAPITAL_USDC"] = float(allocated)

        cfg["ENABLE_BASE_POSITION"] = bool(cfg.get("ENABLE_BASE_POSITION", False))
        base_pos = float(cfg.get("BASE_POSITION_USDC", 0.0) or 0.0)
        if base_pos < 0:
            raise ValueError("BASE_POSITION_USDC must be >= 0")
        cfg["BASE_POSITION_USDC"] = float(base_pos)

        base_spacing = float(cfg.get("BASE_GRID_SPACING", 0.0025))
        base_size = float(cfg.get("BASE_ORDER_SIZE_USDC", 40.0))
        s1 = float(cfg.get("STAGE1_THRESHOLD_USDC", 400.0))
        s2 = float(cfg.get("STAGE2_THRESHOLD_USDC", 600.0))
        s1_exit = float(cfg.get("STAGE1_EXIT_USDC", max(0.0, s1 * 0.95)))
        s2_exit = float(cfg.get("STAGE2_EXIT_USDC", max(0.0, s2 * 0.93)))
        opp_tp = float(cfg.get("STAGE2_OPP_TP_SIZE_USDC", 0.0))
        opp_add = float(cfg.get("STAGE2_OPP_ADD_SIZE_USDC", 0.0))
        if base_spacing <= 0:
            raise ValueError("BASE_GRID_SPACING must be > 0")
        if base_size <= 0:
            raise ValueError("BASE_ORDER_SIZE_USDC must be > 0")
        if s1 <= 0 or s2 <= 0 or s2 <= s1:
            raise ValueError("STAGE thresholds invalid")
        if not (0 <= s1_exit < s1):
            raise ValueError("STAGE1_EXIT_USDC must be < STAGE1_THRESHOLD_USDC")
        if not (0 <= s2_exit < s2):
            raise ValueError("STAGE2_EXIT_USDC must be < STAGE2_THRESHOLD_USDC")
        if opp_tp < 0:
            raise ValueError("STAGE2_OPP_TP_SIZE_USDC must be >= 0")
        if opp_add < 0:
            raise ValueError("STAGE2_OPP_ADD_SIZE_USDC must be >= 0")

        rest_sync = float(cfg.get("REST_SYNC_INTERVAL_SEC", 10.0))
        if rest_sync <= 0:
            raise ValueError("REST_SYNC_INTERVAL_SEC must be > 0")
        cfg["REST_SYNC_INTERVAL_SEC"] = float(rest_sync)

        first_wait = float(cfg.get("ORDER_FIRST_TIME_SEC", 10.0))
        if first_wait < 0:
            raise ValueError("ORDER_FIRST_TIME_SEC must be >= 0")
        cfg["ORDER_FIRST_TIME_SEC"] = float(first_wait)

        cooldown = float(cfg.get("GRID_ACTION_COOLDOWN_SEC", 1.2))
        if cooldown < 0:
            raise ValueError("GRID_ACTION_COOLDOWN_SEC must be >= 0")
        cfg["GRID_ACTION_COOLDOWN_SEC"] = float(cooldown)

        status_itv = float(cfg.get("STATUS_LOG_INTERVAL_SEC", 60.0))
        if status_itv <= 0:
            raise ValueError("STATUS_LOG_INTERVAL_SEC must be > 0")
        cfg["STATUS_LOG_INTERVAL_SEC"] = float(status_itv)

        hs_price = float(cfg.get("HARD_STOPLOSS_PRICE", 0.0) or 0.0)
        if hs_price < 0:
            raise ValueError("HARD_STOPLOSS_PRICE must be >= 0")
        cfg["HARD_STOPLOSS_PRICE"] = float(hs_price)

        cfg["TAKE_PROFIT_ENABLED"] = bool(cfg.get("TAKE_PROFIT_ENABLED", False))
        tp_price = float(cfg.get("TAKE_PROFIT_PRICE", 0.0) or 0.0)
        if tp_price < 0:
            raise ValueError("TAKE_PROFIT_PRICE must be >= 0")
        cfg["TAKE_PROFIT_PRICE"] = float(tp_price)

        cfg["TRAILING_STOP_ENABLED"] = bool(cfg.get("TRAILING_STOP_ENABLED", False))
        base_ratio = float(cfg.get("TRAILING_STOP_BASE_STOP_RATIO", 0.0) or 0.0)
        if base_ratio < 0:
            raise ValueError("TRAILING_STOP_BASE_STOP_RATIO must be >= 0")
        cfg["TRAILING_STOP_BASE_STOP_RATIO"] = float(base_ratio)

        ladder = cfg.get("TRAILING_STOP_LADDER", [])
        if ladder is None:
            ladder = []
        if not isinstance(ladder, list):
            raise ValueError("TRAILING_STOP_LADDER must be a list")
        norm_ladder = []
        for item in ladder:
            if not isinstance(item, dict):
                continue
            trig = item.get("触发盈利比例") if "触发盈利比例" in item else item.get("trigger_ratio")
            stop = item.get("止损盈利比例") if "止损盈利比例" in item else item.get("stop_ratio")
            try:
                trig_f = float(trig)
                stop_f = float(stop)
            except Exception:
                continue
            if trig_f <= 0 or stop_f < 0:
                continue
            if stop_f >= trig_f:
                stop_f = trig_f * 0.95
            norm_ladder.append({"trigger_ratio": float(trig_f), "stop_ratio": float(stop_f)})
        norm_ladder.sort(key=lambda x: float(x["trigger_ratio"]))
        cfg["TRAILING_STOP_LADDER"] = norm_ladder

        pb_ladder = cfg.get("TRAILING_PULLBACK_LADDER", [])
        if pb_ladder is None:
            pb_ladder = []
        if not isinstance(pb_ladder, list):
            raise ValueError("TRAILING_PULLBACK_LADDER must be a list")
        norm_pb = []
        for item in pb_ladder:
            if not isinstance(item, dict):
                continue
            trig = item.get("触发盈利比例") if "触发盈利比例" in item else item.get("trigger_ratio")
            pb = item.get("回撤比例") if "回撤比例" in item else item.get("pullback_ratio")
            try:
                trig_f = float(trig)
                pb_f = float(pb)
            except Exception:
                continue
            if trig_f < 0 or pb_f <= 0 or pb_f >= 1:
                continue
            norm_pb.append({"trigger_ratio": float(trig_f), "pullback_ratio": float(pb_f)})
        norm_pb.sort(key=lambda x: float(x["trigger_ratio"]))
        cfg["TRAILING_PULLBACK_LADDER"] = norm_pb

        cfg["HOT_RELOAD_ENABLED"] = bool(cfg.get("HOT_RELOAD_ENABLED", True))
        watch_itv = float(cfg.get("CONFIG_WATCH_INTERVAL_SEC", 1.0))
        if watch_itv <= 0:
            raise ValueError("CONFIG_WATCH_INTERVAL_SEC must be > 0")
        cfg["CONFIG_WATCH_INTERVAL_SEC"] = float(watch_itv)
        err_itv = float(cfg.get("CONFIG_ERROR_LOG_INTERVAL_SEC", 10.0))
        if err_itv <= 0:
            err_itv = 10.0
        cfg["CONFIG_ERROR_LOG_INTERVAL_SEC"] = float(err_itv)
        return cfg

    def reload_config(self, force: bool = False) -> bool:
        try:
            st = os.stat(self.config_path)
            mtime = float(st.st_mtime)
        except Exception:
            if self._config is None:
                self._config = self.default_config()
                self._config_mtime = None
                self._config_version += 1
                return True
            return False

        if (not force) and (self._config_mtime is not None) and mtime <= float(self._config_mtime):
            return False

        with open(self.config_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("config.json must be a JSON object")
        nested = self._extract_nested_config(raw)
        if nested:
            raw = dict(raw)
            raw.update(nested)
        raw = self._normalize_raw_config(raw)
        cfg = self.default_config()
        cfg.update(raw)
        cfg = self._validate(cfg)
        self._config = cfg
        self._config_mtime = mtime
        self._config_version += 1
        return True

    async def config_watch_loop(self):
        import logging

        log = logging.getLogger()
        while not self.bot.shutdown_event.is_set():
            try:
                changed = False
                async with self.bot.lock:
                    cfg = self.get_config()
                    if not bool(cfg.get("HOT_RELOAD_ENABLED", True)):
                        changed = False
                    else:
                        changed = self.reload_config(force=False)
                    if changed:
                        self.bot._strategy_config_version = int(self._config_version)
                        self.bot._force_orders_resync = True
                if changed:
                    log.info(f"策略配置已热更新: {os.path.basename(self.config_path)} v{self._config_version}")
            except Exception as e:
                now = time.time()
                cooldown = float(self.get_config().get("CONFIG_ERROR_LOG_INTERVAL_SEC", 10.0))
                sig = f"{type(e).__name__}:{str(e)}"
                if sig != self._last_config_error_sig or (now - float(self._last_config_error_ts or 0.0)) >= cooldown:
                    self._last_config_error_sig = sig
                    self._last_config_error_ts = now
                    log.error(f"策略配置热更新失败: {e}")
            await asyncio.sleep(float(self.get_config().get("CONFIG_WATCH_INTERVAL_SEC", 1.0)))

    def _client_id(self, side: str, role: str, stage: int, add_spacing: float, add_usdc: float, tp_spacing: float, tp_usdc: float) -> str:
        prefix = str(self.get_config().get("ORDER_CLIENT_ID_PREFIX", "AF")).strip() or "AF"
        s = str(side or "").strip().lower()
        r = str(role or "").strip().lower()
        base = f"{self._config_version}|{s}|{r}|{int(stage)}|{add_spacing:.8f}|{add_usdc:.4f}|{tp_spacing:.8f}|{tp_usdc:.4f}"
        digest = hashlib.md5(base.encode("utf-8")).hexdigest()[:12]
        side_ch = "L" if s == "long" else "S"
        role_ch = "A" if r == "add" else "T"
        return f"{prefix}{side_ch}{role_ch}{int(stage)}{digest}"

    def _compute_stage(self, side: str, notional_usdc: float) -> int:
        cfg = self.get_config()
        now = time.time()

        s = str(side or "").strip().lower()
        if s == "long":
            last_stage = int(self._last_long_stage)
            last_change = float(self._last_long_stage_change_ts or 0.0)
        else:
            last_stage = int(self._last_short_stage)
            last_change = float(self._last_short_stage_change_ts or 0.0)

        s1 = float(cfg.get("STAGE1_THRESHOLD_USDC", 400.0))
        s2 = float(cfg.get("STAGE2_THRESHOLD_USDC", 600.0))
        s1_exit = float(cfg.get("STAGE1_EXIT_USDC", max(0.0, s1 * 0.95)))
        s2_exit = float(cfg.get("STAGE2_EXIT_USDC", max(0.0, s2 * 0.93)))
        cooldown = float(cfg.get("STAGE_SWITCH_COOLDOWN_SEC", 15.0))
        confirm = max(1, int(cfg.get("STAGE_SWITCH_CONFIRM_COUNT", 2)))

        target = 0
        if notional_usdc >= s2:
            target = 2
        elif notional_usdc >= s1:
            target = 1
        else:
            target = 0

        if last_stage == 2 and notional_usdc < s2_exit:
            target = 1 if notional_usdc >= s1 else 0
        if last_stage == 1 and notional_usdc < s1_exit:
            target = 0

        if target != last_stage and cooldown > 0 and (now - last_change) < cooldown:
            return last_stage

        if s == "long":
            if target == 0 and last_stage == 1:
                self._stage1_down_confirm += 1
                if self._stage1_down_confirm < confirm:
                    return last_stage
            elif target == 1 and last_stage == 0:
                self._stage1_up_confirm += 1
                if self._stage1_up_confirm < confirm:
                    return last_stage
            elif target == 1 and last_stage == 2:
                self._stage2_down_confirm += 1
                if self._stage2_down_confirm < confirm:
                    return last_stage
            elif target == 2 and last_stage in {0, 1}:
                self._stage2_up_confirm += 1
                if self._stage2_up_confirm < confirm:
                    return last_stage
        else:
            if target == 0 and last_stage == 1:
                self._stage1_down_confirm += 1
                if self._stage1_down_confirm < confirm:
                    return last_stage
            elif target == 1 and last_stage == 0:
                self._stage1_up_confirm += 1
                if self._stage1_up_confirm < confirm:
                    return last_stage
            elif target == 1 and last_stage == 2:
                self._stage2_down_confirm += 1
                if self._stage2_down_confirm < confirm:
                    return last_stage
            elif target == 2 and last_stage in {0, 1}:
                self._stage2_up_confirm += 1
                if self._stage2_up_confirm < confirm:
                    return last_stage

        if target != last_stage:
            self._stage1_down_confirm = 0
            self._stage2_down_confirm = 0
            self._stage1_up_confirm = 0
            self._stage2_up_confirm = 0

        if s == "long":
            self._last_long_stage = int(target)
            self._last_long_stage_change_ts = now
        else:
            self._last_short_stage = int(target)
            self._last_short_stage_change_ts = now
        return int(target)

    def side_plan(self, side: str, price: float, position_amount: float) -> dict:
        cfg = self.get_config()
        p = float(price or 0.0)
        pos = float(position_amount or 0.0)
        if p <= 0:
            return {"enabled": False}

        notional = max(0.0, pos * p)
        stage = self._compute_stage(side, notional)

        base_spacing = float(cfg.get("BASE_GRID_SPACING", 0.0025))
        base_usdc = float(cfg.get("BASE_ORDER_SIZE_USDC", 40.0))
        tp_spacing = base_spacing
        tp_usdc = base_usdc
        add_spacing = base_spacing
        add_usdc = base_usdc

        if stage >= 1:
            tp_spacing = float(cfg.get("STAGE1_TP_SPACING", 0.0012))
            tp_usdc = float(cfg.get("STAGE1_TP_SIZE_USDC", base_usdc))
        if stage >= 2:
            add_spacing = float(cfg.get("STAGE2_ADD_SPACING", base_spacing))
            add_usdc = float(cfg.get("STAGE2_ADD_SIZE_USDC", base_usdc))

        s = str(side or "").strip().lower()
        if s == "long":
            add_price = p * (1.0 - add_spacing)
            tp_price = p * (1.0 + tp_spacing)
        else:
            add_price = p * (1.0 + add_spacing)
            tp_price = p * (1.0 - tp_spacing)

        add_qty = self.bot.usdc_to_amount(add_usdc, add_price)
        tp_qty = self.bot.usdc_to_amount(tp_usdc, tp_price)
        if pos > 0 and tp_qty is not None:
            tp_qty = min(float(tp_qty), float(pos))
            tp_qty = self.bot.round_amount(tp_qty)
            if tp_qty < float(self.bot.min_order_amount or 0.0):
                tp_qty = None

        return {
            "enabled": True,
            "side": s,
            "stage": int(stage),
            "notional_usdc": float(notional),
            "add": {
                "price": float(add_price),
                "qty": add_qty,
                "spacing": float(add_spacing),
                "size_usdc": float(add_usdc),
                "client_id": self._client_id(s, "add", stage, add_spacing, add_usdc, tp_spacing, tp_usdc),
            },
            "tp": {
                "price": float(tp_price),
                "qty": tp_qty,
                "spacing": float(tp_spacing),
                "size_usdc": float(tp_usdc),
                "client_id": self._client_id(s, "tp", stage, add_spacing, add_usdc, tp_spacing, tp_usdc),
            },
        }
