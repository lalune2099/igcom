"""Order report sources by run time and retain valid historical prices."""

import gzip
import hashlib
import json
import math
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _fingerprint(path):
    stat = Path(path).stat()
    return [stat.st_size, stat.st_mtime_ns]


def _file_digest(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class HistoryUpdate:
    """Track changed archives and the winning source for each historical price."""

    def __init__(self, output_file, files, root, kind, rebuild=False):
        self.output = Path(output_file)
        self.state_path = self.output.with_suffix(".xlsx.state.json.gz")
        self.kind = kind
        self.state = {"version": 1, "kind": kind, "sources": {}, "owners": {}}
        self.resume = False
        if not rebuild and self.output.exists() and self.state_path.exists():
            try:
                with gzip.open(self.state_path, "rt", encoding="utf-8") as stream:
                    state = json.load(stream)
                if (state["version"] == 1 and state["kind"] == kind
                        and state["digest"] == _file_digest(self.output)
                        and isinstance(state["sources"], dict)
                        and isinstance(state["owners"], dict)):
                    self.state = state
                    self.resume = True
            except (OSError, ValueError, KeyError, EOFError):
                pass
        if self.output.exists() and not self.resume:
            print("{}: rebuilding from archives (requested or no matching state).".format(self.output.name))
        self.changed = []
        self.available_sources = set()
        root = Path(root).resolve()
        for path in files:
            path = Path(path)
            try:
                key = path.resolve().relative_to(root).as_posix()
            except ValueError:
                key = path.resolve().as_posix()
            self.available_sources.add(key)
            previous = self.state["sources"].get(key)
            fingerprint = _fingerprint(path)
            if previous is None or previous["fingerprint"] != fingerprint:
                order = [source_sort_key(path)[0], key]
                self.changed.append((path, key, fingerprint, order))
        self.unchanged = self.resume and not self.changed

    def read_changes(self, read_source):
        """Return only accepted cell changes; blanks never erase valid history."""
        changes = {}
        dates = set()
        sources = self.state["sources"]
        owners = self.state["owners"]
        source_by_id = {item["id"]: item for item in sources.values()}
        key_by_id = {item["id"]: key for key, item in sources.items()}
        next_id = max(source_by_id, default=0) + 1
        for path, key, fingerprint, order in self.changed:
            records, source_dates = read_source(path)
            if _fingerprint(path) != fingerprint:
                raise RuntimeError("Source changed while reading; retry later: {}".format(path))
            dates.update(source_dates)
            source_id = sources[key]["id"] if key in sources else next_id
            if key not in sources:
                next_id += 1
            for record_key, raw_value in records.items():
                value = valid_close(raw_value)
                if value is None:
                    continue
                owner_key = "|".join(str(part) for part in record_key)
                previous_id = owners.get(owner_key)
                previous_order = source_by_id[previous_id]["order"] if previous_id is not None else None
                # A moved run retains its timestamp even when its relative path sorts earlier.
                moved_archive = (previous_order is not None and order[0] == previous_order[0]
                                 and key_by_id[previous_id] not in self.available_sources)
                if previous_id is None or order >= previous_order or moved_archive:
                    changes[record_key] = value
                    owners[owner_key] = source_id
            entry = {"id": source_id, "fingerprint": fingerprint, "order": order}
            sources[key] = entry
            source_by_id[source_id] = entry
            key_by_id[source_id] = key
        print("{}: {} new/changed archives, {} accepted prices.".format(
            self.output.name, len(self.changed), len(changes)))
        return changes, dates

    def save(self, wb):
        """Replace the workbook only after a complete save; bind state to its hash."""
        self.output.parent.mkdir(parents=True, exist_ok=True)
        temporary = []
        try:
            for suffix in (".xlsx", ".json.gz"):
                with tempfile.NamedTemporaryFile(dir=self.output.parent, suffix=suffix,
                                                 prefix=".report-", delete=False) as stream:
                    temporary.append(Path(stream.name))
            wb.save(temporary[0])
            self.state["digest"] = _file_digest(temporary[0])
            with gzip.open(temporary[1], "wt", encoding="utf-8") as stream:
                json.dump(self.state, stream, ensure_ascii=True, separators=(",", ":"))
            os.replace(str(temporary[0]), str(self.output))
            os.replace(str(temporary[1]), str(self.state_path))
        finally:
            for path in temporary:
                if path.exists():
                    path.unlink()


def insert_missing_dates(worksheets, previous_dates, dates, rows_per_date):
    """Insert only missing date blocks, returning the first row block to refresh."""
    previous = set(previous_dates)
    first_insert = len(dates)
    for index, day in enumerate(dates):
        if day not in previous:
            first_insert = min(first_insert, index)
            for ws in worksheets:
                ws.insert_rows(3 + index * rows_per_date, rows_per_date)
    return first_insert


def source_sort_key(path):
    path = Path(path)
    for directory in path.parents:
        if directory.name.startswith("historical_data_"):
            try:
                run_time = datetime.strptime(directory.name, "historical_data_%Y%m%d_%H%M%S")
            except ValueError:
                continue
            stamp = run_time.replace(tzinfo=timezone(timedelta(hours=8))).timestamp()
            return stamp, str(path)
    return path.stat().st_mtime, str(path)


def valid_close(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (ValueError, TypeError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def show_latest_two_dates(ws, date_count, rows_per_date):
    """Hide older date blocks while retaining all prices and formula references."""
    first_visible = 3 + max(0, date_count - 2) * rows_per_date
    for row in range(1, ws.max_row + 1):
        ws.row_dimensions[row].hidden = 3 <= row < first_visible
    ws.freeze_panes = "B3"
    ws.sheet_view.topLeftCell = "A1"
    ws.sheet_view.pane.topLeftCell = "B{}".format(first_visible)
    for selection in ws.sheet_view.selection:
        if selection.pane == "bottomRight":
            selection.activeCell = "B{}".format(first_visible)
            selection.sqref = selection.activeCell
