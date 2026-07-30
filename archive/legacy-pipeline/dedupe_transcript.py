import json
import re
import argparse
from collections import defaultdict, Counter
from math import sqrt
from tqdm import tqdm
import sys
from datetime import datetime

def parse_time(t):
    return datetime.fromisoformat(t.replace("Z", "+00:00"))

def is_contained(inner, outer):
    return (
        parse_time(inner['start']) >= parse_time(outer['start']) and
        parse_time(inner['end']) <= parse_time(outer['end']) and
        (parse_time(inner['end']) - parse_time(inner['start'])) <
        (parse_time(outer['end']) - parse_time(outer['start']))
    )

def filter_contained(entries_by_user):
    filtered_by_user = {}
    for user, utterances in entries_by_user.items():
        sorted_utts = sorted(utterances, key=lambda u: parse_time(u['start']))
        kept = []
        for current in sorted_utts:
            if any(is_contained(current, other) for other in kept):
                continue
            kept.append(current)
        filtered_by_user[user] = kept
    return filtered_by_user

def tokenize(text):
    return re.findall(r"\w+", text.lower())

def canonical(text):
    text = text.lower()
    text = re.sub(r"\bwe're\b", "we are", text)
    text = re.sub(r"\bthat's\b", "that is", text)
    text = re.sub(r"[^\w\s]", "", text)
    return text

def cosine(tokens_a, tokens_b):
    ca = Counter(tokens_a)
    cb = Counter(tokens_b)
    all_keys = set(ca) | set(cb)
    dot = sum(ca[k] * cb[k] for k in all_keys)
    norm_a = sqrt(sum(v*v for v in ca.values()))
    norm_b = sqrt(sum(v*v for v in cb.values()))
    return dot / (norm_a * norm_b) if norm_a and norm_b else 0.0

def score(entry):
    text = entry['text']
    s = len(text)
    s += 10 * text.count('.') + 5 * text.count('!')
    s += 5 if text and text[0].isupper() else 0
    s -= 3 * text.lower().count("uh")
    s -= 3 * text.lower().count("um")
    return s

def fast_cluster_v9(entries):
    indexed_tokens = []
    for idx, entry in enumerate(entries):
        tokens = tokenize(entry['text'])
        indexed_tokens.append((idx, entry, tokens))

    parent = list(range(len(entries)))
    cluster_size = [1] * len(entries)

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            if cluster_size[rx] < cluster_size[ry]:
                rx, ry = ry, rx
            parent[ry] = rx
            cluster_size[rx] += cluster_size[ry]

    MAX_CLUSTER_SIZE = 100
    MIN_TOKENS = 3
    JACCARD_THRESHOLD = 0.80
    COSINE_THRESHOLD = 0.92
    ENABLE_TIME_FILTER = False
    MAX_TIME_DIFF = 60.0

    for i in range(len(indexed_tokens)):
        for j in range(i + 1, len(indexed_tokens)):
            a_idx, a_entry, a_tokens = indexed_tokens[i]
            b_idx, b_entry, b_tokens = indexed_tokens[j]

            if not a_tokens or not b_tokens:
                continue
            if len(a_tokens) < MIN_TOKENS or len(b_tokens) < MIN_TOKENS:
                continue

            if ENABLE_TIME_FILTER:
                if abs(a_entry['start'] - b_entry['start']) > MAX_TIME_DIFF:
                    continue

            if cluster_size[find(a_idx)] > MAX_CLUSTER_SIZE or cluster_size[find(b_idx)] > MAX_CLUSTER_SIZE:
                continue

            len_a = len(a_tokens)
            len_b = len(b_tokens)
            inter = set(a_tokens) & set(b_tokens)
            union_tokens = set(a_tokens) | set(b_tokens)
            jaccard = len(inter) / len(union_tokens)

            if jaccard >= JACCARD_THRESHOLD:
                if len_a < 0.6 * len_b and set(a_tokens).issubset(set(b_tokens)):
                    union(a_idx, b_idx)
                    continue
                if len_b < 0.6 * len_a and set(b_tokens).issubset(set(a_tokens)):
                    union(a_idx, b_idx)
                    continue

            canon_a = canonical(a_entry['text'])
            canon_b = canonical(b_entry['text'])

            if canon_a == canon_b:
                union(a_idx, b_idx)
                continue

            canon_tokens_a = tokenize(canon_a)
            canon_tokens_b = tokenize(canon_b)
            canon_inter = set(canon_tokens_a) & set(canon_tokens_b)
            canon_union = set(canon_tokens_a) | set(canon_tokens_b)
            canon_jaccard = len(canon_inter) / len(canon_union) if canon_union else 0.0
            canon_cosine = cosine(canon_tokens_a, canon_tokens_b)

            if canon_jaccard >= JACCARD_THRESHOLD or canon_cosine >= COSINE_THRESHOLD:
                union(a_idx, b_idx)
                continue

    clusters = defaultdict(list)
    for idx, entry in enumerate(entries):
        root = find(idx)
        clusters[root].append(entry)

    return list(clusters.values())

def deduplicate(entries):
    clusters = fast_cluster_v9(entries)
    initial_best = [max(cluster, key=score) for cluster in clusters]

    seen = []
    final_best = []

    for entry in initial_best:
        canon = canonical(entry['text'])
        tokens = tokenize(canon)
        if not tokens:
            continue

        matched = False
        for prior in seen:
            prior_tokens = tokenize(canonical(prior['text']))
            union = set(tokens) | set(prior_tokens)
            jac = len(set(tokens) & set(prior_tokens)) / len(union) if union else 0.0
            cos = cosine(tokens, prior_tokens)
            if jac >= 0.85 or cos >= 0.92:
                matched = True
                break

        if not matched:
            seen.append(entry)
            final_best.append(entry)

    return final_best, clusters

def process(input_jsonl, output_text, debug_path=None, filter_contained_flag=False):
    try:
        with open(input_jsonl, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"❌ Error: input file '{input_jsonl}' not found.")
        sys.exit(1)

    if not lines:
        print(f"⚠️ Warning: input file '{input_jsonl}' is empty.")
        sys.exit(1)

    speaker_entries = defaultdict(list)
    for line in tqdm(lines, desc="Loading entries"):
        obj = json.loads(line)
        speaker_entries[obj['user']].append(obj)

    if filter_contained_flag:
        speaker_entries = filter_contained(speaker_entries)

    deduped_lines = []
    total_before = 0
    total_after = 0
    user_cluster_stats = {}
    debug_output = []

    for user in tqdm(speaker_entries, desc="Deduplicating per speaker"):
        entries = speaker_entries[user]
        total_before += len(entries)
        best_entries, clusters = deduplicate(entries)
        total_after += len(best_entries)
        user_cluster_stats[user] = (len(entries), len(clusters))
        for entry in best_entries:
            deduped_lines.append((entry['start'], user, entry['text']))

        if debug_path:
            debug_output.append(f"# === Speaker: {user} ===\n")
            for idx, cluster in enumerate(clusters, 1):
                best = max(cluster, key=score)
                debug_output.append(f"Cluster {idx:03d} [{len(cluster)} entries]\n✅ Best: {best['text'].strip()}")
                for member in cluster:
                    debug_output.append(f"- {member['text'].strip()}")
                debug_output.append("")

    deduped_lines.sort()

    with open(output_text, 'w', encoding='utf-8') as f:
        for _, user, text in deduped_lines:
            f.write(f"{user}: {text.strip()}\n")

    if debug_path:
        with open(debug_path, 'w', encoding='utf-8') as f:
            f.write("\n".join(debug_output))

    print("\n📊 Deduplication Summary")
    print(f"Total users: {len(speaker_entries)}")
    print(f"Total utterances loaded: {total_before}")
    print(f"Total deduplicated utterances written: {total_after}")
    print(f"Average deduplication ratio: {total_after / total_before:.2%}\n")

    print("Per-user stats:")
    for user, (before, after) in user_cluster_stats.items():
        print(f"  {user}: {before} → {after} clusters ({after / before:.2%})")

def main():
    parser = argparse.ArgumentParser(description="Deduplicate clustered Whisper transcripts per speaker.")
    parser.add_argument("--input-jsonl", required=True, help="Path to input JSONL transcript file")
    parser.add_argument("--output-text", required=True, help="Path to output deduplicated .txt file")
    parser.add_argument("--debug-clusters", help="Optional path to write detailed cluster breakdown for review")
    parser.add_argument("--filter-contained", action="store_true", help="Enable timestamp containment filtering before deduplication")
    args = parser.parse_args()
    process(args.input_jsonl, args.output_text, args.debug_clusters, filter_contained_flag=args.filter_contained)

if __name__ == "__main__":
    main()
