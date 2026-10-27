# -*- coding: utf-8 -*-
"""Incremental helpers for the scheduled IG daily runs.

The first run of a Beijing calendar day keeps the existing full-fetch flow.
Later runs reuse the latest complete run, fetch a short overlapping window,
and write the same complete output files expected by downstream scripts.
"""

import json
import logging
import math
import os
import re
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pandas as pd


INCREMENTAL_LOOKBACK_HOURS = 3


def configure_price_requests(ig_service, session, start_date, end_date, output_dir):
    """Use the account's API offset, and retain only non-secret request metadata."""
    try:
        offset = float(session["timezoneOffset"])
        if not math.isfinite(offset) or abs(offset) >= 24:
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise RuntimeError("Missing or invalid session timezoneOffset; refusing to guess API timezone")
    if start_date.tzinfo is None or end_date.tzinfo is None:
        raise ValueError("Price request bounds must include a timezone")
    account_timezone = timezone(timedelta(hours=offset))
    start_text = start_date.astimezone(account_timezone).strftime("%Y-%m-%dT%H:%M:%S")
    end_text = end_date.astimezone(account_timezone).strftime("%Y-%m-%dT%H:%M:%S")
    audit_dir = Path(output_dir) / "api_raw"
    audit_dir.mkdir(parents=True, exist_ok=True)
    ig_service._igcom_account_timezone = account_timezone
    ig_service._igcom_audit_dir = audit_dir
    ig_service._igcom_fetch_status = []
    metadata = {
        "account_timezone_offset_hours": offset,
        "request_from": start_text,
        "request_to": end_text,
        "start_utc": start_date.astimezone(timezone.utc).isoformat(),
        "end_utc": end_date.astimezone(timezone.utc).isoformat(),
    }
    (audit_dir / "request_metadata.json").write_text(
        json.dumps(metadata, indent=2), encoding="utf-8")
    print("API account timezone: UTC{:+g}; request: {} to {}".format(offset, start_text, end_text))
    return start_text, end_text


def _valid_price_frame(frame):
    if frame.empty:
        return frame.copy()
    frame = frame.copy()
    frame["Close"] = pd.to_numeric(frame["Close"], errors="coerce")
    return frame.loc[frame["Close"].notna() & frame["Close"].map(math.isfinite)].copy()


def fetch_prices_with_audit(ig_service, epic, resolution, start_text, end_text, formatter, logger):
    """Archive the unmerged API price payload before formatting or deduplication."""
    audit_dir = ig_service._igcom_audit_dir
    name = re.sub(r"[^A-Za-z0-9_.-]", "_", epic + "_" + resolution)
    received_prices = []
    status = {"epic": epic, "resolution": resolution, "status": "failed"}

    def audited_format(prices, version):
        received_prices.extend(prices)
        (audit_dir / (name + ".json")).write_text(
            json.dumps(received_prices, indent=2), encoding="utf-8")
        return formatter(prices, version, account_timezone=ig_service._igcom_account_timezone)

    try:
        response = ig_service.fetch_historical_prices_by_epic(
            epic=epic, resolution=resolution, start_date=start_text,
            end_date=end_text, format=audited_format)
        frame = response["prices"].copy()
        frame["Resolution"] = resolution
        valid = _valid_price_frame(frame)
        status.update(received_points=len(frame), valid_points=len(valid))
        if valid.empty:
            status["status"] = "empty"
            logger.warning("No valid new prices for %s %s; any old prices remain unrefreshed", epic, resolution)
        else:
            status.update(status="ok" if len(valid) == len(frame) else "partial",
                          first_london=str(valid.index.min()), last_london=str(valid.index.max()))
            if len(valid) != len(frame):
                logger.warning("Dropped %s invalid new prices for %s %s", len(frame) - len(valid), epic, resolution)
            for stamp, row in valid.iterrows():
                if stamp.hour == 20 and stamp.minute == 0:
                    print("[Fetched before merge] {} {} {} London Close={}".format(
                        epic, resolution, stamp, row["Close"]))
        return valid
    except Exception as exc:
        status["error"] = str(exc)
        logger.warning("Failed to fetch %s data for %s: %s; old data is not refreshed", resolution, epic, exc)
        return pd.DataFrame()
    finally:
        ig_service._igcom_fetch_status.append(status)
        (audit_dir / "fetch_status.json").write_text(
            json.dumps(ig_service._igcom_fetch_status, indent=2), encoding="utf-8")


def save_fetch_audit(output_dir, incremental_df, previous_df=None, start_date=None, end_date=None):
    """Keep fetched-only rows and distinguish replacements from carried prices."""
    incremental_df.to_csv(Path(output_dir) / "Fetched_Products_1h_30min.csv", encoding="utf-8")
    if previous_df is None or previous_df.empty or incremental_df.empty:
        return
    keys = ["DateTime (London)", "Epic", "Resolution"]
    old = previous_df.reset_index()[keys + ["Close"]].drop_duplicates(keys, keep="last")
    new = incremental_df.reset_index()[keys + ["Close"]].drop_duplicates(keys, keep="last")
    audit = old.merge(new, on=keys, how="outer", suffixes=("_previous", "_fetched"), indicator=True)
    audit["refresh_status"] = audit["_merge"].astype(str).map(
        {"left_only": "carried_forward", "right_only": "new", "both": "refreshed"})
    if start_date is not None and end_date is not None:
        start_london = pd.Timestamp(start_date).tz_convert("Europe/London").tz_localize(None)
        end_london = pd.Timestamp(end_date).tz_convert("Europe/London").tz_localize(None)
        missing = ((audit["_merge"] == "left_only")
                   & audit[keys[0]].between(start_london, end_london))
        audit.loc[missing, "refresh_status"] = "not_refreshed"
        if missing.any():
            logging.getLogger(__name__).warning(
                "%s old prices inside the requested window were not refreshed; see Merge_price_audit.csv",
                int(missing.sum()))
    audit.to_csv(Path(output_dir) / "Merge_price_audit.csv", index=False, encoding="utf-8")
    replaced = audit.loc[audit["_merge"] == "both"]
    changed = (replaced["Close_previous"] - replaced["Close_fetched"]).abs() > 1e-9
    print("Merge audit: {} refreshed keys; {} changed prices; new valid values take priority".format(
        len(replaced), int(changed.sum())))


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


def get_run_output_directory(output_root_dir, run_time):
    """Group runs by their Beijing calendar month, including simulated backfills."""
    beijing = timezone(timedelta(hours=8))
    if run_time.tzinfo is None:
        run_time = run_time.replace(tzinfo=beijing)
    run_time = run_time.astimezone(beijing)
    return os.path.join(
        output_root_dir, "data", run_time.strftime("%Y%m"),
        "historical_data_" + run_time.strftime("%Y%m%d_%H%M%S"),
    )


def find_previous_complete_run(
    output_root_dir: str,
    run_date: str,
    current_time: datetime,
) -> Optional[PreviousRun]:
    """Return the latest earlier run containing both downstream source files."""
    root = Path(output_root_dir)
    if not root.exists():
        return None

    if current_time.tzinfo is not None:
        current_time = current_time.astimezone(timezone(timedelta(hours=8)))
    current_naive = current_time.replace(tzinfo=None)
    combined_name = "All_Products_Full_1h_30min_{}.xlsx".format(run_date)
    final_name = "IG变化率_{}.xlsx".format(run_date)
    candidates = []

    pattern = "historical_data_{}_*".format(run_date)
    directories = list(root.glob(pattern))
    directories.extend((root / "data" / run_date[:6]).glob(pattern))
    for directory in directories:
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
    frames = [_valid_price_frame(frame) for frame in (previous_df, incremental_df) if not frame.empty]
    if not frames:
        return pd.DataFrame()

    # Preserve old-then-new order until deduplication; sorting first can keep old prices.
    merged = pd.concat(frames, ignore_index=False, sort=False)
    index_name = merged.index.name or "DateTime (London)"
    merged.index.name = index_name
    flattened = merged.reset_index()

    dedupe_columns = [index_name, "Epic", "Resolution"]

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
    output_dir = None

    print("=" * 70)
    print("Incremental run: reuse previous complete output")
    print("Previous run : {}".format(previous_run.directory))
    print("Fetch window : {} to {} (Beijing)".format(start_date.isoformat(), end_date.isoformat()))
    print("=" * 70)

    try:
        session = ig_service.create_session()
        output_dir = get_run_output_directory(job.OUTPUT_ROOT_DIR, end_date)
        os.makedirs(output_dir, exist_ok=True)
        start_date_str, end_date_str = configure_price_requests(
            ig_service, session, start_date, end_date, output_dir)

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

        save_fetch_audit(output_dir, incremental_df, previous_df, start_date, end_date)
        if incremental_df.empty:
            raise RuntimeError("No valid new prices fetched; refusing to generate or email an old-only report")
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

