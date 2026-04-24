import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

BASE = Path(__file__).parent
SA_KEY = Path(os.environ.get("SA_KEY_PATH", str(BASE / "service-account.json")))
DEPARTMENTS = BASE / "departments.json"
OUTPUT_SHEET_ID = os.environ["OUTPUT_SHEET_ID"]
STATE_HASH_PATH = Path(os.environ.get("STATE_HASH_PATH", str(BASE / ".state" / "hash.txt")))

WAT = timezone(timedelta(hours=1))

API_RETRIES = 5

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")

LESSON_HEADERS = [
    "Phase", "Date Logged", "Reported By", "Lesson Description", "Category",
    "Impact", "Root Cause", "Recommendation/Action", "Responsible Party",
    "Status", "Process Change", "Follow-Up Date",
]
INCIDENT_HEADERS = [
    "Reported By", "Date", "Time", "Incident Description", "Category",
    "Impact Level", "Status", "Action Taken", "Responsible Party",
    "Follow-Up Date", "Remarks",
]

LESSON_DATE_IDX = [1, 11]
LESSON_IMPACT_IDX = 5
LESSON_STATUS_IDX = 9

INCIDENT_DATE_IDX = [1, 9]
INCIDENT_IMPACT_IDX = 5
INCIDENT_STATUS_IDX = 6

LESSON_COL_WIDTHS = [85, 115, 155, 360, 155, 200, 295, 335, 170, 125, 180, 120]
INCIDENT_COL_WIDTHS = [160, 115, 90, 360, 150, 125, 125, 335, 185, 120, 250]

PALETTE = {
    "title_bg": "1D1D1F",
    "title_fg": "FFFFFF",
    "header_bg": "FFFFFF",
    "header_fg": "1D1D1F",
    "header_rule": "1D1D1F",
    "banner_bg": "FFFFFF",
    "banner_fg": "1D1D1F",
    "row_a": "FFFFFF",
    "row_b": "FAFAFA",
    "text": "1D1D1F",
    "text_muted": "86868B",
    "hairline": "E5E5E7",
    "accent": "1D1D1F",
}

STATUS_TEXT_COLOUR = {
    "Open":         "B25000",
    "In Progress":  "0A66C2",
    "Under Review": "6E44A0",
    "Resolved":     "117A3F",
    "Closed":       "565A63",
}
IMPACT_TEXT_COLOUR = {
    "High":   "C53030",
    "Medium": "B25000",
    "Low":    "117A3F",
}

VALID_IMPACT = {"low": "Low", "medium": "Medium", "high": "High"}
VALID_STATUS = {
    "open": "Open", "closed": "Closed", "under review": "Under Review",
    "in progress": "In Progress", "resolved": "Resolved",
}
MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}
DEFAULT_YEAR = 2026


def sheet_id(url):
    m = ID_RE.search(url)
    return m.group(1) if m else None


def clean_ws(s):
    if "\n" in s:
        return s.strip()
    return re.sub(r"\s+", " ", s).strip()


def try_parse_date(s):
    s = s.strip()
    if not s:
        return None
    if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", s):
        try:
            return datetime.strptime(s, "%Y-%m-%d")
        except ValueError:
            return None
    if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}T.*", s):
        try:
            return datetime.fromisoformat(s.split("T")[0])
        except ValueError:
            return None
    m = re.fullmatch(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", s)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if y < 100:
            y += 2000
        if d <= 12 and mo > 12:
            d, mo = mo, d
        try:
            return datetime(y, mo, d)
        except ValueError:
            return None
    m = re.fullmatch(r"(\d{1,2})[\- ]([A-Za-z]{3,4})[\- ]?(\d{2,4})?", s)
    if m:
        d = int(m.group(1))
        mon_key = m.group(2).lower()[:3]
        if mon_key not in MONTHS:
            return None
        mo = MONTHS[mon_key]
        y = int(m.group(3)) if m.group(3) else DEFAULT_YEAR
        if y < 100:
            y += 2000
        try:
            return datetime(y, mo, d)
        except ValueError:
            return None
    m = re.fullmatch(
        r"(\d{1,2})(?:st|nd|rd|th)[,\s]+([A-Za-z]+)[,\s]+(\d{2,4})",
        s, re.IGNORECASE,
    )
    if m:
        d = int(m.group(1))
        mon_key = m.group(2).lower()[:3]
        if mon_key not in MONTHS:
            return None
        mo = MONTHS[mon_key]
        y = int(m.group(3))
        if y < 100:
            y += 2000
        try:
            return datetime(y, mo, d)
        except ValueError:
            return None
    return None


def format_date(raw):
    if not raw or not str(raw).strip():
        return "", True
    parsed = try_parse_date(str(raw))
    if parsed:
        return parsed.strftime("%d-%b-%Y"), True
    return str(raw).strip(), False


def normalize_impact(raw):
    if not raw:
        return ""
    s = str(raw).strip()
    if len(s) <= 20:
        return VALID_IMPACT.get(s.lower(), s)
    return s


def normalize_status(raw):
    if not raw:
        return ""
    s = str(raw).strip()
    return VALID_STATUS.get(s.lower(), s)


def _load_one(sheets_api, sid, source_cols, date_idx_out, impact_idx_out, status_idx_out):
    """Reads source, filters by non-empty col A (ID), drops col A, normalizes.
    date_idx_out / impact_idx_out / status_idx_out are indices in the OUTPUT row (after dropping col A)."""
    end_col = chr(ord("A") + source_cols - 1)
    resp = sheets_api.spreadsheets().values().get(
        spreadsheetId=sid, range=f"A4:{end_col}200",
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
    ).execute(num_retries=API_RETRIES)
    rows = resp.get("values", [])
    out_rows = []
    out_flags = []
    for row in rows:
        if not row:
            continue
        padded = list(row) + [""] * (source_cols - len(row))
        padded = [clean_ws(str(c)) for c in padded]
        has_id = bool(padded[0])
        tail = padded[1:]
        non_empty_tail = sum(1 for c in tail if c)
        if has_id and non_empty_tail >= 1:
            pass
        elif not has_id and non_empty_tail >= 3:
            pass
        else:
            continue
        padded = tail
        flags = {}
        for ci in date_idx_out:
            display, valid = format_date(padded[ci])
            padded[ci] = display
            flags[ci] = valid
        padded[impact_idx_out] = normalize_impact(padded[impact_idx_out])
        padded[status_idx_out] = normalize_status(padded[status_idx_out])
        out_rows.append(padded)
        out_flags.append(flags)
    return out_rows, out_flags


def load_data(sheets_api, departments):
    lesson_groups, incident_groups = [], []
    for d in departments:
        lrows, lflags = _load_one(
            sheets_api, sheet_id(d["lesson_learned"]),
            source_cols=13,
            date_idx_out=LESSON_DATE_IDX,
            impact_idx_out=LESSON_IMPACT_IDX,
            status_idx_out=LESSON_STATUS_IDX,
        )
        if lrows:
            lesson_groups.append((d["name"], lrows, lflags))

        irows, iflags = _load_one(
            sheets_api, sheet_id(d["incident_log"]),
            source_cols=12,
            date_idx_out=INCIDENT_DATE_IDX,
            impact_idx_out=INCIDENT_IMPACT_IDX,
            status_idx_out=INCIDENT_STATUS_IDX,
        )
        if irows:
            incident_groups.append((d["name"], irows, iflags))
    return lesson_groups, incident_groups


def rgb(hex_code):
    h = hex_code.lstrip("#")
    return {
        "red": int(h[0:2], 16) / 255.0,
        "green": int(h[2:4], 16) / 255.0,
        "blue": int(h[4:6], 16) / 255.0,
    }


def reset_output_sheet(sheets_api):
    meta = sheets_api.spreadsheets().get(spreadsheetId=OUTPUT_SHEET_ID).execute(num_retries=API_RETRIES)
    existing = meta.get("sheets", [])
    by_name = {s["properties"]["title"]: s for s in existing}

    needed = [
        ("Lesson Learned", PALETTE["title_bg"], len(LESSON_HEADERS)),
        ("Incident Log", PALETTE["title_bg"], len(INCIDENT_HEADERS)),
    ]
    needed_titles = {t for t, _, _ in needed}

    add_reqs = []
    keep = {}
    for title, tab_colour, n_cols in needed:
        if title in by_name:
            keep[title] = by_name[title]["properties"]["sheetId"]
        else:
            add_reqs.append({
                "addSheet": {
                    "properties": {
                        "title": title,
                        "tabColor": rgb(tab_colour),
                        "gridProperties": {"rowCount": 600, "columnCount": n_cols},
                    }
                }
            })

    if add_reqs:
        resp = sheets_api.spreadsheets().batchUpdate(
            spreadsheetId=OUTPUT_SHEET_ID, body={"requests": add_reqs}
        ).execute(num_retries=API_RETRIES)
        for r in resp["replies"]:
            p = r["addSheet"]["properties"]
            keep[p["title"]] = p["sheetId"]

    reset_reqs = []
    for title, tab_colour, n_cols in needed:
        sid = keep[title]
        existing_sheet = by_name.get(title)
        if existing_sheet and existing_sheet.get("basicFilter"):
            reset_reqs.append({"clearBasicFilter": {"sheetId": sid}})
        reset_reqs += [
            {"updateCells": {"range": {"sheetId": sid}, "fields": "userEnteredValue,userEnteredFormat"}},
            {"unmergeCells": {"range": {"sheetId": sid}}},
            {"updateSheetProperties": {
                "properties": {
                    "sheetId": sid,
                    "tabColor": rgb(tab_colour),
                    "gridProperties": {"rowCount": 600, "columnCount": n_cols},
                },
                "fields": "tabColor,gridProperties.rowCount,gridProperties.columnCount",
            }},
        ]

    if reset_reqs:
        sheets_api.spreadsheets().batchUpdate(
            spreadsheetId=OUTPUT_SHEET_ID, body={"requests": reset_reqs}
        ).execute(num_retries=API_RETRIES)

    extras = [
        s["properties"]["sheetId"] for s in existing
        if s["properties"]["title"] not in needed_titles
    ]
    if extras:
        sheets_api.spreadsheets().batchUpdate(
            spreadsheetId=OUTPUT_SHEET_ID,
            body={"requests": [{"deleteSheet": {"sheetId": sid}} for sid in extras]},
        ).execute(num_retries=API_RETRIES)

    return keep["Lesson Learned"], keep["Incident Log"]


def _title_row(label, subtitle, n_cols):
    split = max(n_cols - 4, 1)
    label_fmt = {
        "backgroundColor": rgb(PALETTE["title_bg"]),
        "textFormat": {
            "foregroundColor": rgb(PALETTE["title_fg"]),
            "fontSize": 18, "bold": True, "fontFamily": "Helvetica Neue",
        },
        "horizontalAlignment": "LEFT",
        "verticalAlignment": "MIDDLE",
        "padding": {"left": 22, "right": 20, "top": 10, "bottom": 10},
    }
    subtitle_fmt = {
        "backgroundColor": rgb(PALETTE["title_bg"]),
        "textFormat": {
            "foregroundColor": rgb("B8B8BD"),
            "fontSize": 10, "italic": True, "fontFamily": "Helvetica Neue",
        },
        "horizontalAlignment": "RIGHT",
        "verticalAlignment": "MIDDLE",
        "padding": {"left": 20, "right": 22, "top": 10, "bottom": 10},
    }
    bg_only = {"userEnteredFormat": {"backgroundColor": rgb(PALETTE["title_bg"])}}

    values = [{"userEnteredValue": {"stringValue": label}, "userEnteredFormat": label_fmt}]
    values += [bg_only for _ in range(split - 1)]
    values.append({"userEnteredValue": {"stringValue": subtitle}, "userEnteredFormat": subtitle_fmt})
    values += [bg_only for _ in range(n_cols - split - 1)]
    return {"values": values}, split


def _dept_banner_row(label, n_cols):
    fmt = {
        "backgroundColor": rgb(PALETTE["banner_bg"]),
        "textFormat": {
            "foregroundColor": rgb(PALETTE["banner_fg"]),
            "fontSize": 14, "bold": True, "fontFamily": "Helvetica Neue",
        },
        "horizontalAlignment": "LEFT",
        "verticalAlignment": "MIDDLE",
        "padding": {"left": 18, "right": 18, "top": 10, "bottom": 10},
        "borders": {
            "bottom": {"style": "SOLID", "color": rgb(PALETTE["banner_fg"])},
            "top": {"style": "SOLID", "color": rgb(PALETTE["hairline"])},
        },
    }
    values = [{"userEnteredValue": {"stringValue": label}, "userEnteredFormat": fmt}]
    bg_only = {
        "userEnteredFormat": {
            "backgroundColor": rgb(PALETTE["banner_bg"]),
            "borders": {
                "bottom": {"style": "SOLID", "color": rgb(PALETTE["banner_fg"])},
                "top": {"style": "SOLID", "color": rgb(PALETTE["hairline"])},
            },
        }
    }
    values += [bg_only for _ in range(n_cols - 1)]
    return {"values": values}


def build_tab_requests(
    sheet_id_num, title_text, subtitle, headers, groups,
    date_idx, impact_idx, status_idx,
    col_widths,
):
    n_cols = len(headers)
    reqs = []
    grid = []

    title_row, title_split = _title_row(title_text, subtitle, n_cols)
    grid.append(title_row)

    header_cells = []
    for h in headers:
        header_cells.append({
            "userEnteredValue": {"stringValue": h.upper()},
            "userEnteredFormat": {
                "backgroundColor": rgb(PALETTE["header_bg"]),
                "textFormat": {
                    "foregroundColor": rgb(PALETTE["header_fg"]),
                    "fontSize": 10, "bold": True, "fontFamily": "Helvetica Neue",
                },
                "horizontalAlignment": "LEFT",
                "verticalAlignment": "BOTTOM",
                "wrapStrategy": "WRAP",
                "borders": {
                    "bottom": {"style": "SOLID_MEDIUM", "color": rgb(PALETTE["header_rule"])},
                },
                "padding": {"left": 14, "right": 14, "top": 6, "bottom": 8},
            },
        })
    grid.append({"values": header_cells})

    banner_row_indices = []
    hairline_border = {"style": "SOLID", "color": rgb(PALETTE["hairline"])}
    text_colour = rgb(PALETTE["text"])
    muted = rgb(PALETTE["text_muted"])

    current_row = 2
    for dept_name, rows, flags_list in groups:
        grid.append(_dept_banner_row(dept_name, n_cols))
        banner_row_indices.append(current_row)
        current_row += 1

        for row_i, (row, flags) in enumerate(zip(rows, flags_list)):
            row_bg_hex = PALETTE["row_a"] if row_i % 2 == 0 else PALETTE["row_b"]
            cells = []
            for ci, val in enumerate(row):
                italic = False
                bold = False
                colour = text_colour
                cell_bg = rgb(row_bg_hex)

                if ci in date_idx:
                    if not flags.get(ci, True):
                        italic = True
                        colour = muted
                elif ci == impact_idx:
                    c = IMPACT_TEXT_COLOUR.get(val)
                    if c:
                        colour = rgb(c)
                        bold = True
                elif ci == status_idx:
                    c = STATUS_TEXT_COLOUR.get(val)
                    if c:
                        colour = rgb(c)
                        bold = True

                cells.append({
                    "userEnteredValue": {"stringValue": str(val)},
                    "userEnteredFormat": {
                        "backgroundColor": cell_bg,
                        "textFormat": {
                            "fontSize": 10, "fontFamily": "Helvetica Neue",
                            "foregroundColor": colour,
                            "italic": italic, "bold": bold,
                        },
                        "horizontalAlignment": "LEFT",
                        "verticalAlignment": "TOP",
                        "wrapStrategy": "WRAP",
                        "borders": {
                            "bottom": hairline_border,
                        },
                        "padding": {"left": 14, "right": 14, "top": 10, "bottom": 10},
                    },
                })
            grid.append({"values": cells})
            current_row += 1

    reqs.append({
        "updateCells": {
            "start": {"sheetId": sheet_id_num, "rowIndex": 0, "columnIndex": 0},
            "rows": grid,
            "fields": "userEnteredValue,userEnteredFormat",
        }
    })

    reqs.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id_num,
                "startRowIndex": 0, "endRowIndex": 1,
                "startColumnIndex": 0, "endColumnIndex": title_split,
            },
            "mergeType": "MERGE_ALL",
        }
    })
    reqs.append({
        "mergeCells": {
            "range": {
                "sheetId": sheet_id_num,
                "startRowIndex": 0, "endRowIndex": 1,
                "startColumnIndex": title_split, "endColumnIndex": n_cols,
            },
            "mergeType": "MERGE_ALL",
        }
    })
    for br in banner_row_indices:
        reqs.append({
            "mergeCells": {
                "range": {
                    "sheetId": sheet_id_num,
                    "startRowIndex": br, "endRowIndex": br + 1,
                    "startColumnIndex": 0, "endColumnIndex": n_cols,
                },
                "mergeType": "MERGE_ALL",
            }
        })

    for col_idx, width in enumerate(col_widths):
        reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id_num, "dimension": "COLUMNS",
                          "startIndex": col_idx, "endIndex": col_idx + 1},
                "properties": {"pixelSize": width},
                "fields": "pixelSize",
            }
        })

    if current_row > 2:
        reqs.append({
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id_num, "dimension": "ROWS",
                    "startIndex": 2, "endIndex": current_row,
                }
            }
        })

    reqs.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id_num, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
            "properties": {"pixelSize": 64},
            "fields": "pixelSize",
        }
    })
    reqs.append({
        "updateDimensionProperties": {
            "range": {"sheetId": sheet_id_num, "dimension": "ROWS", "startIndex": 1, "endIndex": 2},
            "properties": {"pixelSize": 50},
            "fields": "pixelSize",
        }
    })
    for br in banner_row_indices:
        reqs.append({
            "updateDimensionProperties": {
                "range": {"sheetId": sheet_id_num, "dimension": "ROWS",
                          "startIndex": br, "endIndex": br + 1},
                "properties": {"pixelSize": 52},
                "fields": "pixelSize",
            }
        })

    reqs.append({
        "updateSheetProperties": {
            "properties": {
                "sheetId": sheet_id_num,
                "gridProperties": {"frozenRowCount": 2, "hideGridlines": True},
            },
            "fields": "gridProperties.frozenRowCount,gridProperties.hideGridlines",
        }
    })

    return reqs


def compute_hash(lesson_groups, incident_groups):
    payload = {
        "lessons": [[name, rows] for name, rows, _ in lesson_groups],
        "incidents": [[name, rows] for name, rows, _ in incident_groups],
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def read_previous_hash():
    try:
        return STATE_HASH_PATH.read_text().strip()
    except FileNotFoundError:
        return None


def write_current_hash(h):
    STATE_HASH_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_HASH_PATH.write_text(h + "\n")


def main():
    creds = service_account.Credentials.from_service_account_file(
        str(SA_KEY), scopes=SCOPES
    )
    sheets_api = build("sheets", "v4", credentials=creds, cache_discovery=False)
    departments = json.loads(DEPARTMENTS.read_text())["departments"]

    print("Loading source data...")
    lesson_groups, incident_groups = load_data(sheets_api, departments)
    total_lesson = sum(len(g[1]) for g in lesson_groups)
    total_incident = sum(len(g[1]) for g in incident_groups)
    print(f"  Lesson Learned: {total_lesson} rows across {len(lesson_groups)} depts")
    print(f"  Incident Log:   {total_incident} rows across {len(incident_groups)} depts")

    current_hash = compute_hash(lesson_groups, incident_groups)
    previous_hash = read_previous_hash()
    if previous_hash == current_hash:
        print("Source data unchanged — skipping output sheet update.")
        return

    now_wat = datetime.now(WAT)
    subtitle = f"Updated {now_wat.strftime('%d %b %Y')} · {now_wat.strftime('%-I:%M %p').lower()} WAT"

    print("Resetting output tabs...")
    lesson_sheet_id, incident_sheet_id = reset_output_sheet(sheets_api)

    print("Building formatting + data requests...")
    requests = []
    requests += build_tab_requests(
        sheet_id_num=lesson_sheet_id,
        title_text="Consolidated Lesson Learned",
        subtitle=subtitle,
        headers=LESSON_HEADERS, groups=lesson_groups,
        date_idx=LESSON_DATE_IDX, impact_idx=LESSON_IMPACT_IDX, status_idx=LESSON_STATUS_IDX,
        col_widths=LESSON_COL_WIDTHS,
    )
    requests += build_tab_requests(
        sheet_id_num=incident_sheet_id,
        title_text="Consolidated Incident Log",
        subtitle=subtitle,
        headers=INCIDENT_HEADERS, groups=incident_groups,
        date_idx=INCIDENT_DATE_IDX, impact_idx=INCIDENT_IMPACT_IDX, status_idx=INCIDENT_STATUS_IDX,
        col_widths=INCIDENT_COL_WIDTHS,
    )

    print(f"Applying {len(requests)} requests...")
    CHUNK = 100
    for i in range(0, len(requests), CHUNK):
        sheets_api.spreadsheets().batchUpdate(
            spreadsheetId=OUTPUT_SHEET_ID,
            body={"requests": requests[i:i + CHUNK]},
        ).execute(num_retries=API_RETRIES)

    write_current_hash(current_hash)
    print("Done.")
    print(f"  Output: https://docs.google.com/spreadsheets/d/{OUTPUT_SHEET_ID}/edit")


if __name__ == "__main__":
    main()
