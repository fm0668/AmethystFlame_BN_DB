import json
import os
import re
import subprocess
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


_ROOT = os.path.dirname(os.path.abspath(__file__))
_CONFIGS_DIR = os.path.join(_ROOT, "configs")
_STATUS_DIR = os.path.join(_ROOT, "status")
_SLOTS = ["slot_01", "slot_02", "slot_03"]


def _safe_read_json(path: str):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _safe_write_json_atomic(path: str, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    data = json.dumps(obj, ensure_ascii=False, indent=2)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(data)
    os.replace(tmp, path)


def _pid_alive(pid: int) -> bool:
    if pid is None:
        return False
    try:
        pid_i = int(pid)
    except Exception:
        return False
    if pid_i <= 0:
        return False
    try:
        if os.name == "nt":
            r = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid_i}", "/NH"],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=False,
                text=True,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            out = (r.stdout or "").strip()
            return str(pid_i) in out
        os.kill(pid_i, 0)
        return True
    except Exception:
        return False


def _slot_config_path(slot_id: str) -> str:
    return os.path.join(_CONFIGS_DIR, f"{slot_id}.json")


def _slot_status_path(slot_id: str) -> str:
    return os.path.join(_STATUS_DIR, f"{slot_id}.json")

def _slot_pid_path(slot_id: str) -> str:
    return os.path.join(_STATUS_DIR, f"{slot_id}.pid")


def _slot_stop_flag_path(slot_id: str) -> str:
    return os.path.join(_STATUS_DIR, f"{slot_id}.stop")

def _slot_start_flag_path(slot_id: str) -> str:
    return os.path.join(_STATUS_DIR, f"{slot_id}.start")

def _slot_restart_flag_path(slot_id: str) -> str:
    return os.path.join(_STATUS_DIR, f"{slot_id}.restart")


def _normalize_account_mode(v) -> str:
    s = str(v or "").strip().lower()
    if s in {"testnet", "paper", "sim", "测试网", "测试网络", "测试", "模拟", "仿真"}:
        return "测试网络"
    if s in {"real", "mainnet", "live", "实盘", "正式"}:
        return "实盘"
    return "实盘"


def _normalize_direction(v) -> str:
    s = str(v or "").strip().lower()
    if s in {"做多", "多", "long", "l", "buy"}:
        return "做多"
    if s in {"做空", "空", "short", "s", "sell"}:
        return "做空"
    return "做多"


def _parse_bool(v, default: bool = False) -> bool:
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


def _validate_symbol(coin: str, quote: str):
    c = str(coin or "").strip().upper()
    q = str(quote or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9\u4e00-\u9fff]{1,15}", c or ""):
        raise ValueError("币种格式无效，应为1-15位字母/数字/中文（交易代码），如ETH/SOL/币安人生")
    if q not in {"USDT", "USDC"}:
        raise ValueError("计价只允许USDT或USDC")
    return c, q


def _editable_view(cfg: dict) -> dict:
    grid = cfg.get("网格") if isinstance(cfg.get("网格"), dict) else {}
    funds = cfg.get("资金") if isinstance(cfg.get("资金"), dict) else {}
    base = cfg.get("底仓") if isinstance(cfg.get("底仓"), dict) else {}
    sync = cfg.get("同步") if isinstance(cfg.get("同步"), dict) else {}
    hs = cfg.get("硬止损") if isinstance(cfg.get("硬止损"), dict) else {}
    ts = cfg.get("移动硬止损") if isinstance(cfg.get("移动硬止损"), dict) else {}
    tp = cfg.get("止盈") if isinstance(cfg.get("止盈"), dict) else {}
    inst = cfg.get("实例") if isinstance(cfg.get("实例"), dict) else {}
    pair = cfg.get("交易对") if isinstance(cfg.get("交易对"), dict) else {}
    margin_mode = str(cfg.get("保证金模式") or "").strip()
    if margin_mode not in {"逐仓", "全仓"}:
        margin_mode = "逐仓"
    return {
        "实例": {"启用": _parse_bool(inst.get("启用"), True)},
        "账户模式": _normalize_account_mode(cfg.get("账户模式")),
        "保证金模式": margin_mode,
        "交易对": {
            "币": str(pair.get("币") or "").strip().upper(),
            "计价": str(pair.get("计价") or "").strip().upper(),
        },
        "网格": {
            "方向": _normalize_direction(grid.get("方向")),
            "间距比例": grid.get("间距比例"),
            "每格金额": grid.get("每格金额"),
            "只做MAKER": _parse_bool(grid.get("只做MAKER"), False),
            "杠杆倍数": grid.get("杠杆倍数"),
        },
        "资金": {"分配资金": funds.get("分配资金")},
        "底仓": {"启用": _parse_bool(base.get("启用"), False), "金额": base.get("金额")},
        "同步": {
            "状态同步间隔秒": sync.get("状态同步间隔秒"),
            "最小重挂间隔秒": sync.get("最小重挂间隔秒"),
            "状态日志间隔秒": sync.get("状态日志间隔秒"),
        },
        "硬止损": {"价格": hs.get("价格")},
        "移动硬止损": {
            "启用": _parse_bool(ts.get("启用"), False),
            "初始止损比例": ts.get("初始止损比例"),
            "阶梯": ts.get("阶梯"),
            "回撤阶梯": ts.get("回撤阶梯"),
        },
        "止盈": {"启用": _parse_bool(tp.get("启用"), False), "价格": tp.get("价格")},
    }


def _apply_update(cfg: dict, update: dict) -> dict:
    out = dict(cfg or {})
    inst = out.get("实例") if isinstance(out.get("实例"), dict) else {}
    inst["启用"] = _parse_bool(((update.get("实例") or {}).get("启用")), inst.get("启用", True))
    out["实例"] = inst

    out["账户模式"] = _normalize_account_mode(update.get("账户模式") or out.get("账户模式"))

    mm = str(update.get("保证金模式") or out.get("保证金模式") or "").strip()
    if mm not in {"逐仓", "全仓"}:
        mm = "逐仓"
    out["保证金模式"] = mm

    pair = out.get("交易对") if isinstance(out.get("交易对"), dict) else {}
    up_pair = update.get("交易对") or {}
    coin, quote = _validate_symbol(up_pair.get("币") or pair.get("币"), up_pair.get("计价") or pair.get("计价"))
    pair["币"] = coin
    pair["计价"] = quote
    out["交易对"] = pair

    grid = out.get("网格") if isinstance(out.get("网格"), dict) else {}
    up_grid = update.get("网格") or {}
    grid["方向"] = _normalize_direction(up_grid.get("方向") or grid.get("方向"))
    if "间距比例" in up_grid:
        grid["间距比例"] = float(up_grid.get("间距比例"))
    if "每格金额" in up_grid:
        grid["每格金额"] = float(up_grid.get("每格金额"))
    if "只做MAKER" in up_grid:
        grid["只做MAKER"] = _parse_bool(up_grid.get("只做MAKER"), False)
    if "杠杆倍数" in up_grid and up_grid.get("杠杆倍数") is not None:
        grid["杠杆倍数"] = float(up_grid.get("杠杆倍数"))
    out["网格"] = grid

    funds = out.get("资金") if isinstance(out.get("资金"), dict) else {}
    up_funds = update.get("资金") or {}
    if "分配资金" in up_funds:
        funds["分配资金"] = float(up_funds.get("分配资金"))
    out["资金"] = funds

    base = out.get("底仓") if isinstance(out.get("底仓"), dict) else {}
    up_base = update.get("底仓") or {}
    if "启用" in up_base:
        base["启用"] = _parse_bool(up_base.get("启用"), False)
    if "金额" in up_base and up_base.get("金额") is not None:
        base["金额"] = float(up_base.get("金额"))
    out["底仓"] = base

    sync = out.get("同步") if isinstance(out.get("同步"), dict) else {}
    up_sync = update.get("同步") or {}
    for k in ("状态同步间隔秒", "最小重挂间隔秒", "状态日志间隔秒"):
        if k in up_sync and up_sync.get(k) is not None:
            sync[k] = float(up_sync.get(k))
    out["同步"] = sync

    hs = out.get("硬止损") if isinstance(out.get("硬止损"), dict) else {}
    up_hs = update.get("硬止损") or {}
    if "价格" in up_hs and up_hs.get("价格") is not None:
        hs["价格"] = float(up_hs.get("价格"))
    out["硬止损"] = hs

    ts = out.get("移动硬止损") if isinstance(out.get("移动硬止损"), dict) else {}
    up_ts = update.get("移动硬止损") or {}
    if "启用" in up_ts:
        ts["启用"] = _parse_bool(up_ts.get("启用"), False)
    if "初始止损比例" in up_ts and up_ts.get("初始止损比例") is not None:
        ts["初始止损比例"] = float(up_ts.get("初始止损比例"))
    if "阶梯" in up_ts and up_ts.get("阶梯") is not None:
        ts["阶梯"] = up_ts.get("阶梯")
    if "回撤阶梯" in up_ts and up_ts.get("回撤阶梯") is not None:
        ts["回撤阶梯"] = up_ts.get("回撤阶梯")
    out["移动硬止损"] = ts

    tp = out.get("止盈") if isinstance(out.get("止盈"), dict) else {}
    up_tp = update.get("止盈") or {}
    if "启用" in up_tp:
        tp["启用"] = _parse_bool(up_tp.get("启用"), False)
    if "价格" in up_tp and up_tp.get("价格") is not None:
        v = up_tp.get("价格")
        if str(v).strip() == "":
            v = None
        if v is not None:
            tp["价格"] = float(v)
    out["止盈"] = tp

    return out


_INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Grid Panel</title>
  <style>
    *{box-sizing:border-box}
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial;max-width:1100px;margin:16px auto;padding:0 12px}
    .row{display:flex;gap:12px;flex-wrap:wrap;align-items:flex-start}
    .card{border:1px solid #ddd;border-radius:10px;padding:12px;flex:1;min-width:340px;overflow:hidden}
    .hdr{display:flex;align-items:baseline;justify-content:space-between;gap:10px}
    .hdr-left{display:flex;align-items:baseline;gap:8px;flex-wrap:wrap}
    .badge{font-size:12px;padding:2px 8px;border-radius:999px;border:1px solid #ddd}
    .ok{color:#0a7;border-color:#0a7}
    .bad{color:#c00;border-color:#c00}
    .small{color:#666;font-size:12px}
    .status{margin-top:6px;display:flex;flex-direction:column;gap:2px}
    .kv{display:grid;grid-template-columns:96px 1fr;gap:8px 10px;margin:10px 0}
    .full{grid-column:1 / -1}
    .ladder-row{display:grid;grid-template-columns:1fr 1fr auto;gap:8px;align-items:center;margin:6px 0}
    .ladder-row button{height:28px}
    input,select{width:100%;height:32px;padding:6px 8px;border:1px solid #bbb;border-radius:6px;background:#fff}
    textarea{width:100%;min-height:88px;padding:6px 8px;border:1px solid #bbb;border-radius:6px;resize:vertical}
    button{height:32px;padding:0 10px;border:1px solid #888;border-radius:6px;background:#f7f7f7;cursor:pointer}
    button:active{transform:translateY(1px)}
    .btns{display:flex;gap:8px;flex-wrap:wrap}
    .dirty{color:#c00}
  </style>
</head>
<body>
  <h2>网格面板</h2>
  <div class="small">修改交易对/方向等需重启的参数：先停止，再启动</div>
  <div id="cards" class="row"></div>
<script>
async function api(url, opts){
  const res = await fetch(url, Object.assign({headers:{'Content-Type':'application/json'}}, opts||{}));
  const txt = await res.text();
  let data = null;
  try{ data = txt ? JSON.parse(txt) : null; }catch(e){ data = {error: txt}; }
  if(!res.ok){ throw new Error((data && data.error) ? data.error : ('HTTP '+res.status)); }
  return data;
}
function el(tag, attrs, children){
  const n = document.createElement(tag);
  if(attrs){
    for(const k in attrs){
      if(k==='class') n.className=attrs[k];
      else if(k==='html') n.innerHTML=attrs[k];
      else n.setAttribute(k, attrs[k]);
    }
  }
  (children||[]).forEach(c=> n.appendChild(c));
  return n;
}
function f2(v){
  if(v===null || v===undefined || v==='') return '-';
  const x = Number(v);
  if(Number.isFinite(x)) return x.toFixed(6).replace(/0+$/,'').replace(/\\.$/,'');
  return String(v);
}
function stateCN(state){
  const s = String(state||'').trim().toLowerCase();
  if(!s) return '';
  if(s==='running') return '运行中';
  if(s==='stopped') return '已停止';
  if(s==='shutdown') return '已退出';
  return state;
}
function stateDisplay(slot){
  if(!slot) return '';
  const st = stateCN(slot.state);
  if(st) return st;
  return '';
}
function toNumOrNull(v){
  if(v===null || v===undefined) return null;
  const s = String(v).trim();
  if(!s) return null;
  const x = Number(s);
  return Number.isFinite(x) ? x : null;
}
async function withPending(btn, fn){
  if(!btn) return await fn();
  const old = btn.disabled;
  btn.disabled = true;
  try{ return await fn(); } finally{ btn.disabled = old; }
}

async function resetStatusIfOfflineOrStopped(id){
  const data = await api('/api/slots');
  const slot = (data.slots || []).find(x=>x.id===id);
  const alive = slot ? !!slot.alive : false;
  const st = String((slot && slot.state) || '').trim().toLowerCase();
  const isStopped = (!st) || (st!=='running');
  if(!alive || isStopped){
    await api(`/api/slots/${id}/reset_status`, {method:'POST', body: JSON.stringify({})});
  }
}

const cards = new Map();

function markDirty(id){
  const c = cards.get(id);
  if(!c) return;
  c.dirty = true;
  c.dirtyEl.textContent = '未保存';
  c.dirtyEl.className = 'small dirty';
}

function clearDirty(id){
  const c = cards.get(id);
  if(!c) return;
  c.dirty = false;
  c.dirtyEl.textContent = '';
  c.dirtyEl.className = 'small';
}

async function loadConfigIntoCard(id, force){
  const c = cards.get(id);
  if(!c) return;
  if(c.dirty && !force) return;
  const cfg = await api(`/api/slots/${id}/config`);
  c.mode.value = (cfg['账户模式']||'实盘');
  c.marginMode.value = (cfg['保证金模式']||'逐仓');
  c.coin.value = (cfg['交易对']||{})['币']||'';
  c.quote.value = (cfg['交易对']||{})['计价']||'USDT';
  c.dir.value = ((cfg['网格']||{})['方向']||'做多');
  c.spacing.value = ((cfg['网格']||{})['间距比例'] ?? '');
  c.money.value = ((cfg['网格']||{})['每格金额'] ?? '');
  c.leverage.value = ((cfg['网格']||{})['杠杆倍数'] ?? '');
  c.maker.value = (((cfg['网格']||{})['只做MAKER']===true)?'true':'false');
  c.alloc.value = ((cfg['资金']||{})['分配资金'] ?? '');
  c.baseEnable.value = (((cfg['底仓']||{})['启用']===true)?'true':'false');
  c.baseAmount.value = ((cfg['底仓']||{})['金额'] ?? '');
  c.hard.value = ((cfg['硬止损']||{})['价格'] ?? '');
  c.tsEnable.value = (((cfg['移动硬止损']||{})['启用']===true)?'true':'false');
  c.tsBase.value = ((cfg['移动硬止损']||{})['初始止损比例'] ?? '');
  c.tpEnable.value = (((cfg['止盈']||{})['启用']===true))?'true':'false';
  c.tpPrice.value = ((cfg['止盈']||{})['价格'] ?? '');
  const ladder = ((cfg['移动硬止损']||{})['阶梯'] || []);
  const pbLadder = ((cfg['移动硬止损']||{})['回撤阶梯'] || []);
  c.ladderRows = [];
  c.ladderRowsEl.innerHTML = '';
  if(Array.isArray(ladder)){
    for(const it of ladder){
      const tr = (it && (it['触发盈利比例'] ?? it['trigger_ratio'])) ?? '';
      const sr = (it && (it['止损盈利比例'] ?? it['stop_ratio'])) ?? '';
      c.addLadderRow(tr, sr);
    }
  }
  c.pbRows = [];
  c.pbRowsEl.innerHTML = '';
  if(Array.isArray(pbLadder)){
    for(const it of pbLadder){
      const tr = (it && (it['触发盈利比例'] ?? it['trigger_ratio'])) ?? '';
      const pr = (it && (it['回撤比例'] ?? it['pullback_ratio'])) ?? '';
      c.addPbRow(tr, pr);
    }
  }
  clearDirty(id);
}

function ensureCard(slot){
  const root = document.getElementById('cards');
  if(cards.has(slot.id)) return cards.get(slot.id);

  const card = el('div', {class:'card'});
  const hdr = el('div', {class:'hdr'});
  const hdrLeft = el('div', {class:'hdr-left'});
  const title = el('div', {html:`<b>${slot.id}</b>`});
  const badge = el('span', {class:`badge ${slot.alive?'ok':'bad'}`, html: slot.alive?'在线':'离线'});
  const state = el('span', {class:'small', html: stateDisplay(slot)});
  const dirtyEl = el('span', {class:'small', html:''});
  hdrLeft.appendChild(title);
  hdrLeft.appendChild(badge);
  hdrLeft.appendChild(state);
  hdrLeft.appendChild(dirtyEl);
  hdr.appendChild(hdrLeft);
  card.appendChild(hdr);

  const statusBox = el('div', {class:'status small'});
  const stSymbol = el('div', {html:''});
  const stDir = el('div', {html:''});
  const stMode = el('div', {html:''});
  const stEquity = el('div', {html:''});
  const stPnl = el('div', {html:''});
  const stDd = el('div', {html:''});
  const stNotional = el('div', {html:''});
  const stErr = el('div', {html:''});
  statusBox.appendChild(stSymbol);
  statusBox.appendChild(stDir);
  statusBox.appendChild(stMode);
  statusBox.appendChild(stErr);
  statusBox.appendChild(stEquity);
  statusBox.appendChild(stPnl);
  statusBox.appendChild(stDd);
  statusBox.appendChild(stNotional);
  card.appendChild(statusBox);

  const form = el('div', {class:'kv'});
  const coin = el('input');
  const quote = el('select'); quote.innerHTML = `<option>USDT</option><option>USDC</option>`;
  const dir = el('select'); dir.innerHTML = `<option>做多</option><option>做空</option>`;
  const mode = el('select'); mode.innerHTML = `<option>实盘</option><option>测试网络</option>`;
  const marginMode = el('select'); marginMode.innerHTML = `<option>逐仓</option><option>全仓</option>`;
  const spacing = el('input');
  const money = el('input');
  const leverage = el('input'); leverage.setAttribute('list', `${slot.id}_lev_opts`);
  const levList = el('datalist', {id:`${slot.id}_lev_opts`});
  ['1','2','3','5','10','20','50','100'].forEach(v=>levList.appendChild(el('option',{value:v})));
  const maker = el('select'); maker.innerHTML = `<option value="false">否</option><option value="true">是</option>`;
  const alloc = el('input');
  const baseEnable = el('select'); baseEnable.innerHTML = `<option value="false">否</option><option value="true">是</option>`;
  const baseAmount = el('input');
  const hard = el('input');
  const tsEnable = el('select'); tsEnable.innerHTML = `<option value="false">否</option><option value="true">是</option>`;
  const tsBase = el('input');
  const tpEnable = el('select'); tpEnable.innerHTML = `<option value="false">否</option><option value="true">是</option>`;
  const tpPrice = el('input');
  const ladderWrap = el('div');
  const ladderRowsEl = el('div');
  const ladderAddBtn = el('button', {html:'添加阶梯'});
  ladderAddBtn.type = 'button';
  ladderWrap.appendChild(ladderRowsEl);
  ladderWrap.appendChild(ladderAddBtn);
  const pbWrap = el('div');
  const pbRowsEl = el('div');
  const pbAddBtn = el('button', {html:'添加回撤阶梯'});
  pbAddBtn.type = 'button';
  pbWrap.appendChild(pbRowsEl);
  pbWrap.appendChild(pbAddBtn);
  form.appendChild(el('div',{html:'环境'})); form.appendChild(mode);
  form.appendChild(el('div',{html:'保证金模式'})); form.appendChild(marginMode);
  form.appendChild(el('div',{html:'币(交易代码)'})); form.appendChild(coin);
  form.appendChild(el('div',{html:'计价'})); form.appendChild(quote);
  form.appendChild(el('div',{html:'方向'})); form.appendChild(dir);
  form.appendChild(el('div',{html:'间距比例'})); form.appendChild(spacing);
  form.appendChild(el('div',{html:'每格金额'})); form.appendChild(money);
  form.appendChild(el('div',{html:'杠杆倍数'})); form.appendChild(leverage);
  form.appendChild(el('div',{html:'只做MAKER'})); form.appendChild(maker);
  form.appendChild(el('div',{html:'分配资金'})); form.appendChild(alloc);
  form.appendChild(el('div',{html:'底仓启用'})); form.appendChild(baseEnable);
  form.appendChild(el('div',{html:'底仓金额'})); form.appendChild(baseAmount);
  form.appendChild(el('div',{html:'硬止损价'})); form.appendChild(hard);
  form.appendChild(el('div',{html:'移动硬止损'})); form.appendChild(tsEnable);
  form.appendChild(el('div',{html:'初始止损比例'})); form.appendChild(tsBase);
  form.appendChild(el('div',{html:'移动止损阶梯'})); form.appendChild(ladderWrap);
  form.appendChild(el('div',{html:'跟踪回撤阶梯'})); form.appendChild(pbWrap);
  form.appendChild(el('div',{html:'止盈启用'})); form.appendChild(tpEnable);
  form.appendChild(el('div',{html:'止盈价格'})); form.appendChild(tpPrice);
  card.appendChild(levList);
  card.appendChild(form);

  const btns = el('div', {class:'btns'});
  const saveBtn = el('button',{html:'保存'});
  const startBtn = el('button',{html:'启动'});
  const restartBtn = el('button',{html:'重启'});
  const stopBtn = el('button',{html:'停止'});
  const refreshBtn = el('button',{html:'刷新配置'});
  btns.appendChild(saveBtn);
  btns.appendChild(startBtn);
  btns.appendChild(restartBtn);
  btns.appendChild(stopBtn);
  btns.appendChild(refreshBtn);
  card.appendChild(btns);

  function bindDirty(n){
    n.addEventListener('input', ()=>markDirty(slot.id));
    n.addEventListener('change', ()=>markDirty(slot.id));
  }
  [coin,quote,dir,mode,marginMode,spacing,money,leverage,maker,alloc,baseEnable,baseAmount,hard,tsEnable,tsBase,tpEnable,tpPrice].forEach(bindDirty);

  function addLadderRow(trigger, stop){
    const row = el('div', {class:'ladder-row'});
    const tr = el('input'); tr.value = (trigger ?? '');
    const sr = el('input'); sr.value = (stop ?? '');
    const del = el('button', {html:'删除'}); del.type = 'button';
    row.appendChild(tr);
    row.appendChild(sr);
    row.appendChild(del);
    ladderRowsEl.appendChild(row);
    bindDirty(tr);
    bindDirty(sr);
    del.onclick = ()=>{ row.remove(); markDirty(slot.id); };
    return {tr, sr};
  }
  ladderAddBtn.onclick = ()=>{ addLadderRow('', ''); markDirty(slot.id); };

  function addPbRow(trigger, pullback){
    const row = el('div', {class:'ladder-row'});
    const tr = el('input'); tr.value = (trigger ?? '');
    const pr = el('input'); pr.value = (pullback ?? '');
    const del = el('button', {html:'删除'}); del.type = 'button';
    row.appendChild(tr);
    row.appendChild(pr);
    row.appendChild(del);
    pbRowsEl.appendChild(row);
    bindDirty(tr);
    bindDirty(pr);
    del.onclick = ()=>{ row.remove(); markDirty(slot.id); };
    return {tr, pr};
  }
  pbAddBtn.onclick = ()=>{ addPbRow('', ''); markDirty(slot.id); };

  saveBtn.onclick = async ()=>{
    await withPending(saveBtn, async ()=>{
      try{
        const ladder = [];
        for(const row of Array.from(ladderRowsEl.querySelectorAll('.ladder-row'))){
          const inputs = row.querySelectorAll('input');
          const tr = inputs.length > 0 ? toNumOrNull(inputs[0].value) : null;
          const sr = inputs.length > 1 ? toNumOrNull(inputs[1].value) : null;
          if(tr!=null && sr!=null){
            ladder.push({"触发盈利比例": tr, "止损盈利比例": sr});
          }
        }
        const pb = [];
        for(const row of Array.from(pbRowsEl.querySelectorAll('.ladder-row'))){
          const inputs = row.querySelectorAll('input');
          const tr = inputs.length > 0 ? toNumOrNull(inputs[0].value) : null;
          const pr = inputs.length > 1 ? toNumOrNull(inputs[1].value) : null;
          if(tr!=null && pr!=null){
            pb.push({"触发盈利比例": tr, "回撤比例": pr});
          }
        }
        await api(`/api/slots/${slot.id}/config`, {method:'PUT', body: JSON.stringify({
          "账户模式": mode.value,
          "保证金模式": marginMode.value,
          "交易对": {"币": coin.value, "计价": quote.value},
          "网格": {"方向": dir.value, "间距比例": spacing.value, "每格金额": money.value, "杠杆倍数": leverage.value, "只做MAKER": (maker.value==='true')},
          "资金": {"分配资金": alloc.value},
          "底仓": {"启用": (baseEnable.value==='true'), "金额": baseAmount.value},
          "硬止损": {"价格": hard.value},
          "移动硬止损": {"启用": (tsEnable.value==='true'), "初始止损比例": tsBase.value, "阶梯": ladder, "回撤阶梯": pb},
          "止盈": {"启用": (tpEnable.value==='true'), "价格": toNumOrNull(tpPrice.value)}
        })});
        clearDirty(slot.id);
        await refreshSlotsOnce();
      }catch(e){
        alert(e && e.message ? e.message : String(e));
      }
    });
  };
  startBtn.onclick = async ()=>{ await withPending(startBtn, async ()=>{ try{ await api(`/api/slots/${slot.id}/action`, {method:'POST', body: JSON.stringify({action:'start'})}); await refreshSlotsOnce(); }catch(e){ alert(e && e.message ? e.message : String(e)); } }); };
  restartBtn.onclick = async ()=>{ await withPending(restartBtn, async ()=>{ try{ await api(`/api/slots/${slot.id}/action`, {method:'POST', body: JSON.stringify({action:'restart'})}); await refreshSlotsOnce(); }catch(e){ alert(e && e.message ? e.message : String(e)); } }); };
  stopBtn.onclick = async ()=>{ await withPending(stopBtn, async ()=>{ try{ await api(`/api/slots/${slot.id}/action`, {method:'POST', body: JSON.stringify({action:'stop'})}); await refreshSlotsOnce(); }catch(e){ alert(e && e.message ? e.message : String(e)); } }); };
  refreshBtn.onclick = async ()=>{ await withPending(refreshBtn, async ()=>{ try{ await loadConfigIntoCard(slot.id, true); await resetStatusIfOfflineOrStopped(slot.id); await refreshSlotsOnce(); }catch(e){ alert(e && e.message ? e.message : String(e)); } }); };

  root.appendChild(card);
  const entry = {id:slot.id, card, badge, state, stSymbol, stDir, stMode, stErr, stEquity, stPnl, stDd, stNotional, dirtyEl, mode, marginMode, coin, quote, dir, spacing, money, leverage, maker, alloc, baseEnable, baseAmount, hard, tsEnable, tsBase, tpEnable, tpPrice, ladderRowsEl, ladderRows: [], addLadderRow, pbRowsEl, pbRows: [], addPbRow, dirty:false};
  cards.set(slot.id, entry);
  loadConfigIntoCard(slot.id, false).catch(()=>{});
  return entry;
}

function updateStatus(slot){
  const c = ensureCard(slot);
  c.badge.className = `badge ${slot.alive?'ok':'bad'}`;
  c.badge.textContent = slot.alive?'在线':'离线';
  c.state.textContent = stateDisplay(slot);
  c.stSymbol.textContent = `交易对：${slot.symbol||'-'}`;
  c.stDir.textContent = `方向：${slot.direction||'-'}`;
  c.stMode.textContent = `环境：${slot.account_mode||'实盘'}`;
  c.stErr.textContent = `最后错误：${slot.last_error||'-'}`;
  c.stEquity.textContent = `实例权益：${(slot.equity_usdt!=null)?f2(slot.equity_usdt):'-'}`;
  c.stPnl.textContent = `实例盈亏：${(slot.pnl_usdt!=null)?f2(slot.pnl_usdt):'-'}`;
  c.stDd.textContent = `最大回撤：${(slot.max_drawdown_ratio!=null)?(Number(slot.max_drawdown_ratio)*100).toFixed(2)+'%':'-'}`;
  c.stNotional.textContent = `持仓名义价值：${(slot.position_notional_usdt!=null)?f2(slot.position_notional_usdt):'-'}`;
}

async function refreshSlotsOnce(){
  const data = await api('/api/slots');
  for(const s of data.slots){
    updateStatus(s);
  }
}

let polling = false;
async function poll(){
  if(polling) return;
  polling = true;
  try{
    await refreshSlotsOnce();
  }catch(e){
  }finally{
    polling = false;
  }
}

refreshSlotsOnce().catch(e=>alert(e.message));
setInterval(poll, 2000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def _send(self, status: int, obj, content_type: str = "application/json; charset=utf-8"):
        body = obj
        if isinstance(obj, (dict, list)):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        elif isinstance(obj, str):
            body = obj.encode("utf-8")
        else:
            body = b""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0") or "0")
        except Exception:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        if not raw:
            return {}
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            return {}

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            self._send(200, _INDEX_HTML, "text/html; charset=utf-8")
            return
        if u.path == "/api/slots":
            os.makedirs(_CONFIGS_DIR, exist_ok=True)
            os.makedirs(_STATUS_DIR, exist_ok=True)
            slots = []
            now = time.time()
            for sid in _SLOTS:
                cfg = _safe_read_json(_slot_config_path(sid)) or {}
                st = _safe_read_json(_slot_status_path(sid)) or {}
                ts = st.get("ts")
                alive = False
                sync_itv = None
                try:
                    sync = cfg.get("同步") if isinstance(cfg.get("同步"), dict) else {}
                    sync_itv = float(sync.get("状态同步间隔秒")) if sync.get("状态同步间隔秒") is not None else None
                except Exception:
                    sync_itv = None
                grace = 15.0
                if sync_itv is not None and sync_itv > 0:
                    grace = max(grace, float(sync_itv) * 3.0 + 2.0)
                if ts is None:
                    try:
                        ts = os.path.getmtime(_slot_status_path(sid))
                    except Exception:
                        ts = None
                if ts is not None:
                    try:
                        alive = (now - float(ts)) <= float(grace)
                    except Exception:
                        alive = False
                if not alive:
                    pid = None
                    try:
                        s = open(_slot_pid_path(sid), "r", encoding="utf-8").read().strip()
                        pid = int(s)
                    except Exception:
                        pid = None
                    if pid is not None and _pid_alive(pid):
                        alive = True
                slots.append(
                    {
                        "id": sid,
                        "enabled": _parse_bool(((cfg.get("实例") or {}).get("启用")), True),
                        "account_mode": _normalize_account_mode(cfg.get("账户模式")),
                        "symbol": f"{((cfg.get('交易对') or {}).get('币') or '').upper()}/{((cfg.get('交易对') or {}).get('计价') or '').upper()}",
                        "direction": _normalize_direction(((cfg.get("网格") or {}).get("方向"))),
                        "alive": alive,
                        "state": ((st.get("health") or {}).get("state")) if isinstance(st, dict) else None,
                        "last_error": ((st.get("health") or {}).get("last_error")) if isinstance(st, dict) else None,
                        "equity_usdt": ((st.get("accounting") or {}).get("equity_usdt")) if isinstance(st, dict) else None,
                        "pnl_usdt": ((st.get("accounting") or {}).get("pnl_usdt")) if isinstance(st, dict) else None,
                        "max_drawdown_ratio": ((st.get("accounting") or {}).get("max_drawdown_ratio")) if isinstance(st, dict) else None,
                        "position_notional_usdt": (
                            (abs(float((((st.get("position") or {}).get("amount")) or 0.0))) * float((((st.get("position") or {}).get("mark_price")) or 0.0)))
                            if isinstance(st, dict)
                            else None
                        ),
                    }
                )
            self._send(200, {"slots": slots})
            return
        if u.path.startswith("/api/slots/") and u.path.endswith("/config"):
            sid = u.path.split("/")[3]
            if sid not in _SLOTS:
                self._send(404, {"error": "unknown slot"})
                return
            cfg = _safe_read_json(_slot_config_path(sid)) or {}
            self._send(200, _editable_view(cfg))
            return
        if u.path.startswith("/api/slots/") and u.path.endswith("/status"):
            sid = u.path.split("/")[3]
            if sid not in _SLOTS:
                self._send(404, {"error": "unknown slot"})
                return
            st = _safe_read_json(_slot_status_path(sid)) or {}
            self._send(200, st)
            return
        self._send(404, {"error": "not found"})

    def do_PUT(self):
        u = urlparse(self.path)
        if u.path.startswith("/api/slots/") and u.path.endswith("/config"):
            sid = u.path.split("/")[3]
            if sid not in _SLOTS:
                self._send(404, {"error": "unknown slot"})
                return
            update = self._read_json_body()
            cur = _safe_read_json(_slot_config_path(sid)) or {}
            try:
                new_cfg = _apply_update(cur, update or {})
            except Exception as e:
                self._send(400, {"error": str(e)})
                return
            _safe_write_json_atomic(_slot_config_path(sid), new_cfg)
            self._send(200, {"ok": True})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        u = urlparse(self.path)
        if u.path.startswith("/api/slots/") and u.path.endswith("/reset_status"):
            sid = u.path.split("/")[3]
            if sid not in _SLOTS:
                self._send(404, {"error": "unknown slot"})
                return
            try:
                os.remove(_slot_status_path(sid))
            except Exception:
                pass
            try:
                os.remove(os.path.join(_STATUS_DIR, f"{sid}_debug.json"))
            except Exception:
                pass
            self._send(200, {"ok": True})
            return
        if u.path.startswith("/api/slots/") and u.path.endswith("/action"):
            sid = u.path.split("/")[3]
            if sid not in _SLOTS:
                self._send(404, {"error": "unknown slot"})
                return
            body = self._read_json_body()
            action = str((body or {}).get("action") or "").strip().lower()
            cfg_path = _slot_config_path(sid)
            cfg = _safe_read_json(cfg_path) or {}
            inst = cfg.get("实例") if isinstance(cfg.get("实例"), dict) else {}
            if action == "start":
                inst["启用"] = True
                cfg["实例"] = inst
                _safe_write_json_atomic(cfg_path, cfg)
                try:
                    os.remove(_slot_stop_flag_path(sid))
                except Exception:
                    pass
                try:
                    os.makedirs(_STATUS_DIR, exist_ok=True)
                    with open(_slot_start_flag_path(sid), "w", encoding="utf-8") as f:
                        f.write(str(time.time()))
                except Exception:
                    pass
                self._send(200, {"ok": True})
                return
            if action == "stop":
                inst["启用"] = False
                cfg["实例"] = inst
                _safe_write_json_atomic(cfg_path, cfg)
                try:
                    os.makedirs(_STATUS_DIR, exist_ok=True)
                    with open(_slot_stop_flag_path(sid), "w", encoding="utf-8") as f:
                        f.write(str(time.time()))
                except Exception:
                    pass
                try:
                    os.remove(_slot_restart_flag_path(sid))
                except Exception:
                    pass
                try:
                    os.remove(_slot_start_flag_path(sid))
                except Exception:
                    pass
                self._send(200, {"ok": True})
                return
            if action == "restart":
                inst["启用"] = True
                cfg["实例"] = inst
                _safe_write_json_atomic(cfg_path, cfg)
                try:
                    os.remove(_slot_stop_flag_path(sid))
                except Exception:
                    pass
                try:
                    os.makedirs(_STATUS_DIR, exist_ok=True)
                    with open(_slot_restart_flag_path(sid), "w", encoding="utf-8") as f:
                        f.write(str(time.time()))
                except Exception:
                    pass
                try:
                    os.makedirs(_STATUS_DIR, exist_ok=True)
                    with open(_slot_start_flag_path(sid), "w", encoding="utf-8") as f:
                        f.write(str(time.time()))
                except Exception:
                    pass
                self._send(200, {"ok": True})
                return
            self._send(400, {"error": "invalid action"})
            return
        self._send(404, {"error": "not found"})


def main():
    host = os.getenv("PANEL_HOST", "127.0.0.1")
    port = int(os.getenv("PANEL_PORT", "8080") or "8080")
    os.makedirs(_CONFIGS_DIR, exist_ok=True)
    os.makedirs(_STATUS_DIR, exist_ok=True)
    server = ThreadingHTTPServer((host, port), Handler)
    server.serve_forever()


if __name__ == "__main__":
    main()

