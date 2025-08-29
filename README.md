'''
import re
import collections
import os
import statistics
import pandas as pd

# --------- CONFIG ---------
TARGET_COLLECTION = "test_collection"  # <-- change if needed
# --------------------------

def extract_patterns(log_file):
    """Extracts query/QTime stats from INFO logs (for target collection with FST_NM/LST_NM)
       and collects WARN message counts."""
    query_data = collections.defaultdict(lambda: collections.defaultdict(list))
    warn_messages = collections.Counter()

    # Regex patterns
    info_log_regex   = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+INFO\b")
    warn_log_regex   = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\s+WARN\b")

    query_regex      = re.compile(r"[?&]q=([^&\s]+)")
    qtime_regex      = re.compile(r"QTime=(\d+)")
    collection_regex = re.compile(r"[?&]collection=([^&\s]+)")
    sort_regex       = re.compile(r"[?&]sort=([^&\s]+)")

    # Name fields inside query
    name_field_regex = re.compile(r"\b(?:FST_NM|LST_NM)\s*:\s*([A-Za-z0-9_\-']+\*?)", re.IGNORECASE)

    if not os.path.exists(log_file):
        print(f"❌ File not found: {log_file}")
        return None, None

    with open(log_file, 'r', encoding='utf-8-sig', errors='ignore') as file:
        for line in file:
            if info_log_regex.search(line):
                qtime_match      = qtime_regex.search(line)
                query_match      = query_regex.search(line)
                collection_match = collection_regex.search(line)
                sort_match       = sort_regex.search(line)

                if not (query_match and qtime_match and collection_match):
                    continue

                raw_query = query_match.group(1).strip()
                collection = collection_match.group(1).strip()

                # Filter by target collection
                if collection != TARGET_COLLECTION:
                    continue

                # Require FST_NM or LST_NM
                if not name_field_regex.search(raw_query):
                    continue

                qtime = int(qtime_match.group(1))
                sort_pattern = sort_match.group(1) if sort_match else "default"

                query_pattern = normalize_query(raw_query)

                query_data[collection][(query_pattern, sort_pattern)].append(qtime)

            elif warn_log_regex.search(line):
                # Keep whole WARN line
                warn_messages[line.strip()] += 1

    return query_data, warn_messages

def normalize_query(query):
    """Replaces dynamic values in queries with placeholders to detect patterns."""
    query = re.sub(r"\b\d+\b", "<NUM>", query)                                   # numbers
    query = re.sub(r"\b(YES|NO|TRUE|FALSE)\b", "<VAL>", query, flags=re.IGNORECASE)  # booleans
    query = re.sub(r"\b[A-Fa-f0-9]{8,}\b", "<ID>", query)                        # ids/guid-like
    query = re.sub(r"([a-zA-Z_]+):([^\s&]+)", r"\1:<VAL>", query)                # key:value pairs
    return query

def compute_qtime_statistics(query_data):
    """Computes count, min, max, and avg QTime per (collection, query pattern, sort pattern)."""
    result = []
    for collection, queries in query_data.items():
        for (query_pattern, sort_pattern), qtimes in queries.items():
            result.append({
                "Collection": collection,
                "Query Pattern": query_pattern,
                "Sort Pattern": sort_pattern,
                "Count": len(qtimes),
                "Min QTime (ms)": min(qtimes),
                "Max QTime (ms)": max(qtimes),
                "Avg QTime (ms)": round(statistics.mean(qtimes), 2),
            })
    return result

def main(log_file):
    query_data, warn_messages = extract_patterns(log_file)

    if not query_data:
        print("⚠️ No matching INFO log entries found for the target collection and name fields.")
        return

    stats = compute_qtime_statistics(query_data)
    df = pd.DataFrame(stats)
    if not df.empty:
        df = df.sort_values(by=["Collection", "Avg QTime (ms)"], ascending=[True, False])
        import ace_tools as tools
        tools.display_dataframe_to_user(
            name=f"QTime Stats - {TARGET_COLLECTION} (INFO with FST_NM/LST_NM)",
            dataframe=df
        )
    else:
        print("⚠️ No rows after filtering; DataFrame is empty.")

    if warn_messages:
        print("\n=== Top WARN Messages ===")
        for w, cnt in warn_messages.most_common(10):
            print(f"{cnt}x {w}")

if __name__ == "__main__":
    log_file_path = r"C:\Users\adity\Downloads\test.log"  # Update path
    main(log_file_path)

'''
