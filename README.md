```
import re
import os
import glob
import collections
import statistics
import pandas as pd

# --------- CONFIG ---------
LOG_FOLDER = r"C:\Users\adity\Downloads\logs"   # <-- update to your folder
TARGET_COLLECTION = "test_collection"           # <-- change if needed
OUTPUT_FILE = "log_analysis.csv"
# --------------------------

def extract_patterns_from_folder(folder_path):
    """
    Scan all .log/.out files in a folder.
    INFO: Keep only lines from TARGET_COLLECTION whose q contains FST_NM:* or LAST_NAME:* (wildcard required).
    WARN: Count occurrences.
    """
    # (collection) -> ((query_pattern, raw_query, sort), list[qtime])
    info_data = collections.defaultdict(lambda: collections.defaultdict(list))
    warn_counts = collections.Counter()

    # --- Regexes ---
    info_log = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+INFO\b")
    warn_log = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+WARN\b")

    q_param   = re.compile(r"[?&]q=([^&\s]+)")
    qtime     = re.compile(r"QTime=(\d+)")
    sort_param= re.compile(r"[?&]sort=([^&\s]+)")

    # collection can be either param or header like [c:test_collection
    coll_param  = re.compile(r"[?&]collection=([^&\s]+)")
    coll_header = re.compile(r"\[c:\s*([^\s\]]+)", re.IGNORECASE)

    # Require wildcard for these fields (case-insensitive)
    # e.g., FST_NM:DAN*, LAST_NAME:smith*
    name_with_wild = re.compile(r"\b(?:FST_NM|LAST_NAME)\s*:\s*([A-Za-z0-9_\-']+)\*", re.IGNORECASE)

    paths = sorted(glob.glob(os.path.join(folder_path, "*.log")) +
                   glob.glob(os.path.join(folder_path, "*.out")))

    for path in paths:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            for line in f:
                # WARN collection
                if warn_log.search(line):
                    warn_counts[line.strip()] += 1
                    continue

                # INFO collection
                if not info_log.search(line):
                    continue

                q_m  = q_param.search(line)
                qt_m = qtime.search(line)
                if not (q_m and qt_m):
                    continue

                # figure collection
                c_m = coll_param.search(line)
                if not c_m:
                    c_h = coll_header.search(line)
                    collection = c_h.group(1) if c_h else None
                else:
                    collection = c_m.group(1)
                if not collection or collection != TARGET_COLLECTION:
                    continue

                raw_q = q_m.group(1).strip()

                # Must contain FST_NM:* or LAST_NAME:* specifically (wildcard required)
                if not name_with_wild.search(raw_q):
                    continue

                qtime_ms = int(qt_m.group(1))
                s_m = sort_param.search(line)
                sort_val = s_m.group(1) if s_m else "default"

                # Build query pattern that PRESERVES FST_NM/LAST_NAME and masks only their values → <VAL>*
                pattern = normalize_query_preserve_name_wildcards(raw_q)

                info_data[collection][(pattern, raw_q, sort_val)].append(qtime_ms)

    return info_data, warn_counts

def normalize_query_preserve_name_wildcards(query: str) -> str:
    """
    Create a normalized pattern while preserving FST_NM and LAST_NAME keys with wildcard:
    - FST_NM:Something*  -> FST_NM:<VAL>*
    - LAST_NAME:Smith*   -> LAST_NAME:<VAL>*
    Then apply general masking to the rest (numbers, booleans, ids, other key:values).
    """
    # 1) Protect name wildcards first (so later masks don't strip the '*')
    query = re.sub(r"\b(FST_NM|LAST_NAME)\s*:\s*[^\s&]*\*", r"\1:<VAL>*", query, flags=re.IGNORECASE)

    # 2) General masks for the rest
    query = re.sub(r"\b\d+\b", "<NUM>", query)  # numbers
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)  # booleans
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)  # long hex-like ids
    # Generic key:value (leave keys, mask values) — avoids clobbering the '*' we already protected
    query = re.sub(r"([A-Za-z_]+):([^\s&]+)", r"\1:<VAL>", query)

    return query

def compute_rows(info_data, warn_counts):
    """Build CSV rows with required columns."""
    rows = []

    # INFO rows
    for collection, bucket in info_data.items():
        for (pattern, raw_query, sort_val), qtimes in bucket.items():
            rows.append({
                "Collection": collection,
                "Log_Type": "INFO",
                "Query_Pattern": pattern,
                "Query": raw_query,
                "Sort": sort_val,
                "Count": len(qtimes),
                "Min_QTime": min(qtimes),
                "Max_QTime": max(qtimes),
                "Avg_QTime": round(statistics.mean(qtimes), 2),
            })

    # WARN rows (kept for completeness; if you want only INFO, you can remove this block)
    for msg, cnt in warn_counts.items():
        rows.append({
            "Collection": TARGET_COLLECTION,
            "Log_Type": "WARN",
            "Query_Pattern": "",
            "Query": msg,
            "Sort": "",
            "Count": cnt,
            "Min_QTime": "",
            "Max_QTime": "",
            "Avg_QTime": "",
        })

    return rows

def main():
    if not os.path.isdir(LOG_FOLDER):
        print(f"❌ Folder not found: {LOG_FOLDER}")
        return

    info_data, warn_counts = extract_patterns_from_folder(LOG_FOLDER)
    rows = compute_rows(info_data, warn_counts)

    df = pd.DataFrame(rows, columns=[
        "Collection","Log_Type","Query_Pattern","Query","Sort",
        "Count","Min_QTime","Max_QTime","Avg_QTime"
    ])
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Saved: {OUTPUT_FILE}")
    # print a peek
    with pd.option_context("display.max_colwidth", 200):
        print(df.head(10))

if __name__ == "__main__":
    main()

```
