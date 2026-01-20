import asyncio
import os
import signal

import grid_Stablize_BN_DB01 as core


if __name__ == "__main__":
    os.environ["STRATEGY_DIRECTION"] = "short"
    bot = None
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        strategy_config_path = os.getenv("STRATEGY_CONFIG_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json"))
        account_mode = core._get_account_mode(strategy_config_path)
        params = core._get_strategy_params(strategy_config_path)
        if account_mode in {"testnet", "paper", "sim"}:
            bot = core.GridTradingBot(
                core.TESTNET_API_KEY,
                core.TESTNET_API_SECRET,
                params["coin_name"],
                params["contract_type"],
                core.GRID_SPACING,
                core.INITIAL_QUANTITY,
                core.LEVERAGE,
                account_mode,
                params["rest_sync_interval_sec"],
                params["order_first_time_sec"],
            )
        else:
            bot = core.GridTradingBot(
                core.API_KEY,
                core.API_SECRET,
                params["coin_name"],
                params["contract_type"],
                core.GRID_SPACING,
                core.INITIAL_QUANTITY,
                core.LEVERAGE,
                account_mode,
                params["rest_sync_interval_sec"],
                params["order_first_time_sec"],
            )

        def _handle(sig, _frame=None):
            if bot is None:
                return
            if bot.shutdown_event.is_set():
                return
            loop.call_soon_threadsafe(
                lambda: asyncio.create_task(bot.shutdown(f"signal_{getattr(sig, 'name', str(sig))}"))
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

