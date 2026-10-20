# -*- coding: utf-8 -*-
"""Incremental helpers for the scheduled IG daily runs.

The first run of a Beijing calendar day keeps the existing full-fetch flow.
Later runs reuse the latest complete run, fetch a short overlapping window,
and write the same complete output files expected by downstream scripts.
"""

import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd


INCREMENTAL_LOOKBACK_HOURS = 3


@dataclass(frozen=True)
class PreviousRun:
    directory: Path
    run_time: datetime
    combined_excel: Path
    final_workbook: Path


def _parse_run_directory_time(path: Path) -> Optional[datetime]:
    prefix = "historical_data_"
    if not path.name.startswith(prefix):
        return None
    try:
        return datetime.strptime(path.name[len(prefix):], "%Y%m%d_%H%M%S")
    except ValueError:
        return None


def find_previous_complete_run(
    output_root_dir: str,
    run_date: str,
    current_time: datetime,
) -> Optional[PreviousRun]:
    """Return the latest earlier run containing both downstream source files."""
    root = Path(output_root_dir)
    if not root.exists():
        return None

    current_naive = current_time.replace(tzinfo=None)
    combined_name = "All_Products_Full_1h_30min_{}.xlsx".format(run_date)
    final_name = "IG变化率_{}.xlsx".format(run_date)
    candidates = []

    for directory in root.glob("historical_data_{}_*".format(run_date)):
        if not directory.is_dir():
            continue
        run_time = _parse_run_directory_time(directory)
        if run_time is None or run_time >= current_naive:
            continue

        combined_excel = directory / combined_name
        final_workbook = directory / final_name
        if combined_excel.exists() and final_workbook.exists():
            candidates.append(
                PreviousRun(
                    directory=directory,
                    run_time=run_time,
                    combined_excel=combined_excel,
                    final_workbook=final_workbook,
                )
            )

    if not candidates:
        return None
    return max(candidates, key=lambda item: item.run_time)


def _merge_historical_frames(previous_df: pd.DataFrame, incremental_df: pd.DataFrame) -> pd.DataFrame:
    frames = [frame for frame in (previous_df, incremental_df) if not frame.empty]
    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=False, sort=False).sort_index()
    index_name = merged.index.name or "DateTime (London)"
    merged.index.name = index_name
    flattened = merged.reset_index()

    dedupe_columns = [index_name]
    for column in ("Epic", "Resolution"):
        if column in flattened.columns:
            dedupe_columns.append(column)

    flattened = flattened.drop_duplicates(subset=dedupe_columns, keep="last")
    return flattened.set_index(index_name).sort_index()


def _write_complete_historical_outputs(job, merged_df: pd.DataFrame, output_dir: str):
    run_date = job.RUN_DATE
    all_data = {}

    if "Epic" in merged_df.columns:
        for epic, frame in merged_df.groupby("Epic"):
            all_data[epic] = frame.sort_index()

    for epic, frame in all_data.items():
        product_name = job.EPIC_TO_NAME.get(epic, epic)
        safe_filename = job.safe_sheet_name(product_name).replace(" ", "_").replace("$", "USD")
        csv_path = os.path.join(
            output_dir,
            "{}_full_1h_30min_{}.csv".format(safe_filename, run_date),
        )
        frame.to_csv(csv_path, encoding="utf-8")

    combined_csv = os.path.join(
        output_dir,
        "All_Products_Full_1h_30min_{}.csv".format(run_date),
    )
    merged_df.to_csv(combined_csv, encoding="utf-8")

    combined_excel = os.path.join(
        output_dir,
        "All_Products_Full_1h_30min_{}.xlsx".format(run_date),
    )
    with pd.ExcelWriter(combined_excel, engine="openpyxl") as writer:
        merged_df.to_excel(writer, sheet_name="All_Full_Data", index=True)
        for epic, frame in all_data.items():
            sheet_name = job.safe_sheet_name(job.EPIC_TO_NAME.get(epic, epic))
            frame.to_excel(writer, sheet_name=sheet_name, index=True)

    return all_data, os.path.abspath(output_dir), os.path.abspath(combined_excel)


def get_incremental_historical_prices(job, epic_list, previous_run: PreviousRun):
    """Fetch a short overlap and merge it into the previous complete raw workbook."""
    retryer = job.Retrying(
        wait=job.wait_exponential(),
        retry=job.retry_if_exception_type(job.ApiExceededException),
    )
    ig_service = job.IGService(
        job.IGConfig.username,
        job.IGConfig.password,
        job.IGConfig.api_key,
        job.IGConfig.acc_type,
        retryer=retryer,
        use_rate_limiter=True,
    )

    end_date = job.datetime.now(job.TZ_BEIJING)
    start_date = end_date - timedelta(hours=INCREMENTAL_LOOKBACK_HOURS)
    start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%S")
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%S")
    output_dir = None

    print("=" * 70)
    print("Incremental run: reuse previous complete output")
    print("Previous run : {}".format(previous_run.directory))
    print("Fetch window : {} to {} (Beijing)".format(start_date_str, end_date_str))
    print("=" * 70)

    try:
        ig_service.create_session()
        output_dir = os.path.join(
            job.OUTPUT_ROOT_DIR,
            "historical_data_{}".format(job.datetime.now().strftime("%Y%m%d_%H%M%S")),
        )
        os.makedirs(output_dir, exist_ok=True)

        incremental_frames = []
        for index, epic in enumerate(epic_list, 1):
            product_name = job.EPIC_TO_NAME.get(epic, epic)
            print("Incremental {}/{}: {}".format(index, len(epic_list), product_name))

            resolution_frames = []
            for resolution in ("1h", "30Min"):
                frame = job.fetch_data_by_resolution(
                    ig_service,
                    epic,
                    resolution,
                    start_date_str,
                    end_date_str,
                )
                if not frame.empty:
                    resolution_frames.append(frame)

            if not resolution_frames:
                continue

            product_frame = pd.concat(resolution_frames).sort_index()
            product_frame["Product Name"] = product_name
            product_frame["Epic"] = epic
            incremental_frames.append(product_frame)

        previous_df = pd.read_excel(
            previous_run.combined_excel,
            sheet_name="All_Full_Data",
            index_col="DateTime (London)",
            parse_dates=["DateTime (London)"],
        )
        if incremental_frames:
            incremental_df = pd.concat(incremental_frames, ignore_index=False).sort_index()
        else:
            incremental_df = pd.DataFrame()

        merged_df = _merge_historical_frames(previous_df, incremental_df)
        if merged_df.empty:
            raise RuntimeError("Incremental merge produced no historical data")

        return _write_complete_historical_outputs(job, merged_df, output_dir)
    finally:
        try:
            ig_service.logout()
        except Exception:
            pass
        print("Session Closed")


def prepare_updated_template(job, previous_run: Optional[PreviousRun], output_file: str):
    """Carry the prior filled workbook forward, or use the original template fallback."""
    if previous_run is None:
        return job.update_template_dates_uk(job.TEMPLATE_FILE, output_file)

    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(str(previous_run.final_workbook), output_file)
    print("Cumulative template copied from: {}".format(previous_run.final_workbook))
    return output_file

