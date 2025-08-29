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
    """Scan all .log/.out files in a folder. Collect INFO query/QTime stats for the target collection
    where q contains FST_NM or LAST_NAME, and count WARN messages."""
    query_data   = collections.defaultdict(lambda: collections.defaultdict(list))
    raw_queries  = collections.defaultdict(lambda: collections.defaultdict(list))
    warn_counts  = collections.Counter()

    # Regexes
    info_log_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+INFO\b")
    warn_log_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+WARN\b")

    query_regex      = re.compile(r"[?&]q=([^&\s]+)")
    qtime_regex      = re.compile(r"QTime=(\d+)")
    collection_param = re.compile(r"[?&]collection=([^&\s]+)")
    collection_header = re.compile(r"\[c:\s*([^\s\]]+)", re.IGNORECASE)
    sort_regex       = re.compile(r"[?&]sort=([^&\s]+)")

    name_field_regex = re.compile(r"\b(?:FST_NM|LAST_NAME)\s*:\s*([A-Za-z0-9_\-']+\*?)", re.IGNORECASE)

    paths = sorted(glob.glob(os.path.join(folder_path, "*.log")) +
                   glob.glob(os.path.join(folder_path, "*.out")))

    for path in paths:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            for line in f:
                # WARN messages
                if warn_log_regex.search(line):
                    warn_counts[line.strip()] += 1
                    continue

                # INFO messages
                if not info_log_regex.search(line):
                    continue

                q_match = query_regex.search(line)
                qt_match = qtime_regex.search(line)

                # Collection may be param or header
                c_match = collection_param.search(line)
                if not c_match:
                    c_hdr = collection_header.search(line)
                    collection = c_hdr.group(1) if c_hdr else None
                else:
                    collection = c_match.group(1)

                if not (q_match and qt_match and collection):
                    continue

                if collection != TARGET_COLLECTION:
                    continue

                raw_query = q_match.group(1).strip()

                # Must contain FST_NM or LAST_NAME
                if not name_field_regex.search(raw_query):
                    continue

                qtime = int(qt_match.group(1))
                sort_match = sort_regex.search(line)
                sort_pattern = sort_match.group(1) if sort_match else "default"

                query_pattern = normalize_query(raw_query)

                # Save both normalized + raw queries
                query_data[collection][(query_pattern, raw_query, sort_pattern)].append(qtime)

    return query_data, warn_counts

def normalize_query(query: str) -> str:
    """Replace dynamic values in queries with placeholders to detect patterns."""
    query = re.sub(r"\b\d+\b", "<NUM>", query)
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)
    query = re.sub(r"([A-Za-z_]+):([^\s&]+)", r"\1:<VAL>", query)
    return query

def compute_statistics(query_data, warn_counts):
    """Build rows for CSV with INFO and WARN results."""
    rows = []

    # INFO rows
    for collection, entries in query_data.items():
        for (query_pattern, raw_query, sort_pattern), qtimes in entries.items():
            rows.append({
                "Collection": collection,
                "Log_Type": "INFO",
                "Query_Pattern": query_pattern,
                "Query": raw_query,
                "Sort": sort_pattern,
                "Count": len(qtimes),
                "Min_QTime": min(qtimes),
                "Max_QTime": max(qtimes),
                "Avg_QTime": round(statistics.mean(qtimes), 2),
            })

    # WARN rows
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

    query_data, warn_counts = extract_patterns_from_folder(LOG_FOLDER)
    stats = compute_statistics(query_data, warn_counts)

    df = pd.DataFrame(stats, columns=[
        "Collection", "Log_Type", "Query_Pattern", "Query", "Sort",
        "Count", "Min_QTime", "Max_QTime", "Avg_QTime"
    ])

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Results saved to {OUTPUT_FILE}")
    print(df.head(10))

if __name__ == "__main__":
    main()

```
