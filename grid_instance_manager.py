import json
import os
import signal
import subprocess
import sys
import time


def _safe_read_json(path: str) -> dict:
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    return {}


def _normalize_direction(value) -> str:
    s = str(value or "").strip().lower()
    if s in {"做多", "多", "long", "l", "buy"}:
        return "long"
    if s in {"做空", "空", "short", "s", "sell"}:
        return "short"
    return "long"


def _direction_from_config(path: str) -> str:
    cfg = _safe_read_json(path)
    grid = cfg.get("网格") if isinstance(cfg.get("网格"), dict) else {}
    direction = None
    if isinstance(grid, dict):
        direction = grid.get("方向")
    if direction is None:
        direction = cfg.get("方向") or cfg.get("交易方向") or cfg.get("DIRECTION")
    return _normalize_direction(direction)

def _enabled_from_config(path: str) -> bool:
    cfg = _safe_read_json(path)
    inst = cfg.get("实例") if isinstance(cfg.get("实例"), dict) else {}
    v = None
    if isinstance(inst, dict):
        v = inst.get("启用")
    if v is None:
        v = cfg.get("启用") or cfg.get("enabled")
    if v is None:
        return True
    if isinstance(v, bool):
        return bool(v)
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on", "是"}:
        return True
    if s in {"0", "false", "no", "n", "off", "否"}:
        return False
    return True


def _python_exe() -> str:
    return sys.executable or "python"


def _spawn_instance(config_path: str, direction: str) -> subprocess.Popen:
    env = os.environ.copy()
    env["STRATEGY_CONFIG_PATH"] = os.path.abspath(config_path)
    env["STRATEGY_DIRECTION"] = str(direction)
    env["INSTANCE_ID"] = os.path.splitext(os.path.basename(config_path))[0]
    script = "grid_single_long.py" if direction == "long" else "grid_single_short.py"
    creationflags = 0
    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP
    return subprocess.Popen(
        [_python_exe(), script],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env=env,
        creationflags=creationflags,
    )


def _stop_process(proc: subprocess.Popen, timeout_sec: float = 60.0):
    if proc is None:
        return
    if proc.poll() is not None:
        return
    started = time.time()
    try:
        if os.name == "nt":
            proc.send_signal(signal.CTRL_BREAK_EVENT)
            time.sleep(0.5)
        else:
            proc.send_signal(signal.SIGTERM)
    except Exception:
        pass
    deadline = time.time() + float(timeout_sec or 0.0)
    terminated = False
    while time.time() < deadline:
        if proc.poll() is not None:
            return
        time.sleep(0.2)
        if os.name == "nt" and (not terminated) and (time.time() - started) >= 15.0:
            terminated = True
            try:
                proc.terminate()
            except Exception:
                pass
    try:
        proc.kill()
    except Exception:
        pass


def main():
    configs_dir = os.getenv("GRID_CONFIG_DIR", "").strip()
    if not configs_dir:
        configs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs")
    configs_dir = os.path.abspath(configs_dir)
    os.makedirs(configs_dir, exist_ok=True)

    scan_interval_sec = float(os.getenv("GRID_MANAGER_SCAN_INTERVAL_SEC", "1") or 1.0)
    if scan_interval_sec <= 0:
        scan_interval_sec = 1.0

    fixed_slots = os.getenv("GRID_FIXED_SLOTS", "").strip()
    allow_any = os.getenv("GRID_ALLOW_ANY_CONFIG", "").strip()
    fixed_enabled = True if fixed_slots == "" else (fixed_slots not in {"0", "false", "no", "off"})
    allow_any_enabled = allow_any in {"1", "true", "yes", "on"}

    procs = {}
    proc_meta = {}
    stop_deadlines = {}
    stopping = False
    status_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "status")
    os.makedirs(status_dir, exist_ok=True)
    for sid in ("slot_01", "slot_02", "slot_03"):
        try:
            os.remove(os.path.join(status_dir, f"{sid}.start"))
        except Exception:
            pass

    def _pid_path_for_config(path: str) -> str:
        sid = os.path.splitext(os.path.basename(path))[0]
        return os.path.join(status_dir, f"{sid}.pid")

    def _read_pid(path: str):
        try:
            s = open(path, "r", encoding="utf-8").read().strip()
            return int(s)
        except Exception:
            return None

    def _write_pid(path: str, pid: int):
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(str(int(pid)))
        except Exception:
            pass

    def _restart_flag_path_for_config(path: str) -> str:
        sid = os.path.splitext(os.path.basename(path))[0]
        return os.path.join(status_dir, f"{sid}.restart")

    def _start_flag_path_for_config(path: str) -> str:
        sid = os.path.splitext(os.path.basename(path))[0]
        return os.path.join(status_dir, f"{sid}.start")

    def _pid_exists(pid: int) -> bool:
        if pid is None:
            return False
        try:
            if os.name == "nt":
                r = subprocess.run(
                    ["tasklist", "/FI", f"PID eq {int(pid)}", "/NH"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    text=True,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                out = (r.stdout or "").strip()
                return str(int(pid)) in out
            os.kill(int(pid), 0)
            return True
        except Exception:
            return False

    def _kill_pid(pid: int) -> bool:
        if pid is None:
            return True
        try:
            if os.name == "nt":
                r = subprocess.run(
                    ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
                )
                return int(getattr(r, "returncode", 1) or 1) == 0
            else:
                os.kill(int(pid), signal.SIGKILL)
                return True
        except Exception:
            return False

    def _shutdown(*_args):
        nonlocal stopping
        stopping = True

    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        try:
            signal.signal(sig, _shutdown)
        except Exception:
            pass

    while not stopping:
        try:
            files = []
            for name in os.listdir(configs_dir):
                if not name.lower().endswith(".json"):
                    continue
                if fixed_enabled and (not allow_any_enabled):
                    low = name.lower()
                    if low not in {"slot_01.json", "slot_02.json", "slot_03.json"}:
                        continue
                files.append(os.path.join(configs_dir, name))
        except Exception:
            files = []

        desired = {}
        for path in files:
            try:
                os.stat(path)
            except Exception:
                continue
            if not _enabled_from_config(path):
                continue
            if not os.path.exists(_start_flag_path_for_config(path)):
                continue
            direction = _direction_from_config(path)
            desired[path] = {"direction": direction}

        for name in ("slot_01.pid", "slot_02.pid", "slot_03.pid"):
            pid_path = os.path.join(status_dir, name)
            if not os.path.exists(pid_path):
                continue
            cfg_path = os.path.join(configs_dir, name.replace(".pid", ".json"))
            if cfg_path in desired:
                continue
            proc = procs.get(cfg_path)
            if proc is not None and proc.poll() is None:
                continue
            pid = _read_pid(pid_path)
            if pid is None:
                try:
                    os.remove(pid_path)
                except Exception:
                    pass
                continue
            if not _pid_exists(pid):
                try:
                    os.remove(pid_path)
                except Exception:
                    pass
                continue
            _kill_pid(pid)
            if not _pid_exists(pid):
                try:
                    os.remove(pid_path)
                except Exception:
                    pass

        for path, meta in list(proc_meta.items()):
            if path not in desired:
                proc = procs.get(path)
                if proc is None:
                    procs.pop(path, None)
                    proc_meta.pop(path, None)
                    stop_deadlines.pop(path, None)
                    continue
                if proc.poll() is None:
                    if path not in stop_deadlines:
                        grace = float(os.getenv("GRID_SOFT_STOP_GRACE_SEC", "45") or 45.0)
                        if grace <= 0:
                            grace = 45.0
                        stop_deadlines[path] = time.time() + grace
                    if time.time() < float(stop_deadlines.get(path) or 0.0):
                        continue
                _stop_process(proc)
                procs.pop(path, None)
                proc_meta.pop(path, None)
                stop_deadlines.pop(path, None)
                continue

        for path, d in list(desired.items()):
            flag_path = _restart_flag_path_for_config(path)
            if not os.path.exists(flag_path):
                continue
            proc = procs.get(path)
            if proc is not None and proc.poll() is None:
                _stop_process(proc)
                procs.pop(path, None)
                proc_meta.pop(path, None)
                stop_deadlines.pop(path, None)
            try:
                os.remove(flag_path)
            except Exception:
                pass

        for path, d in desired.items():
            proc = procs.get(path)
            if proc is not None and proc.poll() is None:
                continue
            try:
                sid = os.path.splitext(os.path.basename(path))[0]
                stop_flag = os.path.join(status_dir, f"{sid}.stop")
                if os.path.exists(stop_flag):
                    os.remove(stop_flag)
            except Exception:
                pass
            try:
                flag_path = _restart_flag_path_for_config(path)
                if os.path.exists(flag_path):
                    os.remove(flag_path)
            except Exception:
                pass
            procs[path] = _spawn_instance(path, d["direction"])
            try:
                _write_pid(_pid_path_for_config(path), procs[path].pid)
            except Exception:
                pass
            proc_meta[path] = {"direction": d["direction"]}
            stop_deadlines.pop(path, None)

        time.sleep(scan_interval_sec)

    for path, proc in list(procs.items()):
        _stop_process(proc)
        procs.pop(path, None)
        proc_meta.pop(path, None)
        try:
            os.remove(_pid_path_for_config(path))
        except Exception:
            pass


if __name__ == "__main__":
    main()

