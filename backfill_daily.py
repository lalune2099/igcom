# -*- coding: utf-8 -*-
"""
补跑缺失日期的数据。

默认只需要修改 BEIJING_TIME，然后运行：
    python3 backfill_daily.py
"""

import importlib
import os
from datetime import datetime as RealDateTime
from datetime import timedelta, timezone
from typing import Optional


# 只改这里：填要模拟的北京时间，格式必须是 YYYY-MM-DD HH:MM:SS
BEIJING_TIME = "2026-05-09 04:05:00"

# 默认使用 all.py。需要换账号脚本时，可改成 all2、all3、allMon。
SCRIPT_MODULE = "all"

VALID_SCRIPT_MODULES = {"all", "all2", "all3", "allMon"}
BEIJING_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def _normalize_script_module(script_module: str) -> str:
    module_name = script_module.strip()
    if module_name.endswith(".py"):
        module_name = module_name[:-3]
    if module_name not in VALID_SCRIPT_MODULES:
        raise ValueError(
            "SCRIPT_MODULE must be one of: all, all2, all3, allMon"
        )
    return module_name


def _localize_datetime(naive_dt: RealDateTime, tzinfo) -> RealDateTime:
    if hasattr(tzinfo, "localize"):
        return tzinfo.localize(naive_dt)
    return naive_dt.replace(tzinfo=tzinfo)


def parse_beijing_time(value: str, tzinfo=None) -> RealDateTime:
    try:
        parsed = RealDateTime.strptime(value.strip(), BEIJING_TIME_FORMAT)
    except ValueError as exc:
        raise ValueError(
            "BEIJING_TIME must use format YYYY-MM-DD HH:MM:SS, "
            'for example "2026-05-09 04:05:00"'
        ) from exc

    if tzinfo is None:
        tzinfo = timezone(timedelta(hours=8))
    return _localize_datetime(parsed, tzinfo)


def configure_job_module(job, simulated_beijing_time: RealDateTime) -> str:
    run_date = simulated_beijing_time.strftime("%Y%m%d")

    class FixedDateTimeMeta(type):
        def __instancecheck__(cls, instance):
            return isinstance(instance, RealDateTime)

    class FixedDateTime(RealDateTime, metaclass=FixedDateTimeMeta):
        @classmethod
        def now(cls, tz=None):
            if tz is None:
                return simulated_beijing_time.replace(tzinfo=None)
            return simulated_beijing_time.astimezone(tz)

    job.datetime = FixedDateTime
    job.RUN_DATE = run_date
    job.UPDATED_TEMPLATE_FILE_NAME = f"IG变化率_模版更新_{run_date}.xlsx"
    job.FILLED_OUTPUT_FILE_NAME = f"IG变化率_{run_date}.xlsx"
    job.SEND_EMAIL = False
    return run_date


def run_backfill(
    beijing_time: Optional[str] = None,
    script_module: Optional[str] = None,
) -> str:
    module_name = _normalize_script_module(
        script_module or os.getenv("BACKFILL_SCRIPT_MODULE", SCRIPT_MODULE)
    )
    job = importlib.import_module(module_name)
    time_text = beijing_time or os.getenv("BACKFILL_BEIJING_TIME", BEIJING_TIME)
    simulated_beijing_time = parse_beijing_time(time_text, job.TZ_BEIJING)
    run_date = configure_job_module(job, simulated_beijing_time)

    print("=" * 70)
    print("Backfill mode: email disabled")
    print(f"Script       : {module_name}.py")
    print(f"Beijing time : {simulated_beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output date  : {run_date}")
    print("=" * 70)

    job.main()
    return run_date


def main() -> None:
    run_backfill()


if __name__ == "__main__":
    main()
