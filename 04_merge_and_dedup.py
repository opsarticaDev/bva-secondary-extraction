# 04_merge_and_dedup.py
# Stage 4: Merge Stage 2 + Stage 2b (UTF-16 reclassified) and deduplicate.
# Stage 2b rows replace Stage 2 rows for the same file_path.

import csv
import os
import datetime
from collections import Counter
_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── CONFIG ────────────────────────────────────────────────────────────────────
STAGE2_MAIN   = os.path.join(_ROOT, "stage2_extractions.csv")
STAGE2B_PATCH = os.path.join(_ROOT, "stage2b_extractions.csv")
STAGE1B_PATCH = os.path.join(_ROOT, "stage1b_reclassified.csv")
OUTPUT_CSV    = os.path.join(_ROOT, "stage4_merged.csv")
STATS_CSV     = os.path.join(_ROOT, "stage4_stats.csv")
LOG_PATH      = os.path.join(_ROOT, "stage4_merge.log")
# ─────────────────────────────────────────────────────────────────────────────

FIELDNAMES = [
    'file_path', 'file_name', 'citation_nr', 'citation_type',
    'template_type', 'confidence',
    'vlj_name', 'attorney_name', 'representation',
    'issue_count', 'issues',
    'outcome', 'outcome_detail',
    'error', 'source'
]


def main():
    log_lines = []
    def log(msg):
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'{ts} | {msg}'
        log_lines.append(line)
        print(line, flush=True)

    log('Stage 4: Merge and Deduplication starting')

    # Load Stage 2b patch (UTF-16 reclassified extractions) as a lookup
    patch = {}
    if os.path.exists(STAGE2B_PATCH):
        with open(STAGE2B_PATCH, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                patch[row['file_path']] = row
        log(f'Stage 2b patch loaded: {len(patch):,} rows')
    else:
        log(f'No Stage 2b patch found at {STAGE2B_PATCH}. Merging Stage 2 only.')

    # Process Stage 2 main, replacing rows where Stage 2b has a result
    total = 0
    patched = 0
    dupes = 0
    seen_paths = set()

    outcome_counts = Counter()
    template_counts = Counter()
    vlj_found = 0
    atty_found = 0
    rep_found = 0
    issues_found = 0

    with open(STAGE2_MAIN, 'r', encoding='utf-8') as fin, \
         open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=FIELDNAMES, extrasaction='ignore')
        writer.writeheader()

        for row in reader:
            total += 1
            fpath = row['file_path']

            # Dedup by file_path (should not happen, but guard against it)
            if fpath in seen_paths:
                dupes += 1
                continue
            seen_paths.add(fpath)

            # Replace with Stage 2b if available
            if fpath in patch:
                row = patch.pop(fpath)
                row['source'] = 'stage2b'
                patched += 1
            else:
                row['source'] = 'stage2'

            writer.writerow(row)

            # Stats
            template_counts[row.get('template_type', '')] += 1
            outcome_counts[row.get('outcome', '')] += 1
            if row.get('vlj_name'):
                vlj_found += 1
            if row.get('attorney_name'):
                atty_found += 1
            if row.get('representation'):
                rep_found += 1
            if row.get('issues') and row.get('issue_count', '0') != '0':
                issues_found += 1

            if total % 100000 == 0:
                log(f'Processed: {total:,} | Patched: {patched:,}')

        # Write any remaining Stage 2b rows not in Stage 2 (unlikely but safe)
        extra = 0
        for fpath, row in patch.items():
            if fpath not in seen_paths:
                row['source'] = 'stage2b_only'
                writer.writerow(row)
                seen_paths.add(fpath)
                extra += 1
                template_counts[row.get('template_type', '')] += 1
                outcome_counts[row.get('outcome', '')] += 1

    final = total - dupes + extra
    log(f'DONE: Stage 2 rows: {total:,} | Patched from 2b: {patched:,} | '
        f'Dupes removed: {dupes:,} | Extra from 2b: {extra:,} | Final: {final:,}')

    log(f'VLJ found: {vlj_found:,} ({vlj_found/final*100:.1f}%)')
    log(f'Attorney found: {atty_found:,} ({atty_found/final*100:.1f}%)')
    log(f'Representation found: {rep_found:,} ({rep_found/final*100:.1f}%)')
    log(f'Issues found: {issues_found:,} ({issues_found/final*100:.1f}%)')

    log('Template distribution:')
    for k, v in template_counts.most_common():
        label = k if k else '(empty)'
        log(f'  {label:<30} {v:>10,}  ({v/final*100:.2f}%)')

    log('Outcome distribution:')
    for k, v in outcome_counts.most_common():
        label = k if k else '(empty)'
        log(f'  {label:<20} {v:>10,}  ({v/final*100:.2f}%)')

    # Write per-template stats
    with open(STATS_CSV, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['template_type', 'count', 'pct'])
        for k, v in template_counts.most_common():
            w.writerow([k or '(empty)', v, f'{v/final*100:.2f}'])

    log(f'Stats written: {STATS_CSV}')

    with open(LOG_PATH, 'w', encoding='utf-8') as lf:
        lf.write('\n'.join(log_lines))


if __name__ == '__main__':
    main()
