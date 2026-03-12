# 05_qa_validation.py
# Stage 5: QA Validation
# Two passes: (1) automated field-level quality checks with per-template
# extraction rates, (2) stratified sample generation for manual review.

import csv
import os
import re
import random
import datetime
from collections import Counter, defaultdict
_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV       = os.path.join(_ROOT, "stage4_merged.csv")
REPORT_PATH     = os.path.join(_ROOT, "stage5_qa_report.txt")
OUTPUT_SAMPLE   = os.path.join(_ROOT, "stage5_qa_sample.csv")
RESULTS_CSV     = os.path.join(_ROOT, "stage5_qa_results.csv")
LOG_PATH        = os.path.join(_ROOT, "stage5_qa.log")
SAMPLE_PER_TYPE = 50    # Files per template type
RANDOM_SEED     = 42
# ─────────────────────────────────────────────────────────────────────────────

RE_NAME_CHECK = re.compile(r'^[A-Z][A-Za-z.\s\'-]+$')


def main():
    random.seed(RANDOM_SEED)

    log_lines = []
    def log(msg):
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'{ts} | {msg}'
        log_lines.append(line)
        print(line, flush=True)

    log('Stage 5: QA Validation starting')

    if not os.path.exists(INPUT_CSV):
        log(f'ERROR: {INPUT_CSV} not found. Run Stage 4 first.')
        return

    # Pass 1: automated quality checks
    total = 0
    error_count = 0
    by_type = defaultdict(list)

    per_template = defaultdict(lambda: {
        'total': 0, 'vlj': 0, 'atty': 0, 'rep': 0, 'issues': 0, 'outcome': 0
    })
    outcome_counts = Counter()
    source_counts = Counter()
    issue_count_dist = Counter()
    suspicious_vlj = []

    with open(INPUT_CSV, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            total += 1
            tt = row.get('template_type', '')
            by_type[tt].append(row)

            per_template[tt]['total'] += 1
            source_counts[row.get('source', '')] += 1

            if row.get('error'):
                error_count += 1

            vlj = row.get('vlj_name', '').strip()
            atty = row.get('attorney_name', '').strip()
            rep = row.get('representation', '').strip()
            ic = int(row.get('issue_count', 0) or 0)
            outcome = row.get('outcome', '').strip()

            if vlj:
                per_template[tt]['vlj'] += 1
                if not RE_NAME_CHECK.match(vlj) and len(suspicious_vlj) < 50:
                    suspicious_vlj.append((row['file_name'], vlj))
            if atty:
                per_template[tt]['atty'] += 1
            if rep:
                per_template[tt]['rep'] += 1
            if ic > 0:
                per_template[tt]['issues'] += 1
                issue_count_dist[ic] += 1
            if outcome:
                per_template[tt]['outcome'] += 1
                outcome_counts[outcome] += 1

    # Aggregate totals
    vlj_total = sum(d['vlj'] for d in per_template.values())
    atty_total = sum(d['atty'] for d in per_template.values())
    rep_total = sum(d['rep'] for d in per_template.values())
    issues_total = sum(d['issues'] for d in per_template.values())
    outcome_total = sum(d['outcome'] for d in per_template.values())

    # Build report
    lines = []
    lines.append('=' * 80)
    lines.append('BVA EXTRACTION PIPELINE: STAGE 5 QA REPORT')
    lines.append(f'Generated: {datetime.datetime.now().isoformat()}')
    lines.append(f'Input: {INPUT_CSV}')
    lines.append(f'Total records: {total:,}')
    lines.append(f'Errors: {error_count:,}')
    lines.append('=' * 80)
    lines.append('')

    lines.append('EXTRACTION RATES (AGGREGATE)')
    lines.append('')
    for name, count in [('VLJ', vlj_total), ('Attorney', atty_total),
                        ('Representation', rep_total), ('Issues', issues_total),
                        ('Outcome', outcome_total)]:
        pct = count / total * 100 if total else 0
        lines.append(f'  {name:<20} {count:>10,} / {total:,}  ({pct:.1f}%)')
    lines.append('')

    lines.append('EXTRACTION RATES BY TEMPLATE TYPE')
    lines.append('')
    for tt in sorted(per_template.keys(), key=lambda x: -per_template[x]['total']):
        d = per_template[tt]
        t = d['total']
        lines.append(f'  {tt} (n={t:,}):')
        for field, label in [('vlj','VLJ'), ('atty','Attorney'), ('rep','Rep'),
                             ('issues','Issues'), ('outcome','Outcome')]:
            pct = d[field] / t * 100 if t else 0
            lines.append(f'    {label:<12} {d[field]:>10,}  ({pct:.1f}%)')
        lines.append('')

    lines.append('TEMPLATE DISTRIBUTION')
    lines.append('')
    for tt in sorted(per_template.keys(), key=lambda x: -per_template[x]['total']):
        v = per_template[tt]['total']
        lines.append(f'  {tt:<30} {v:>10,}  ({v/total*100:.2f}%)')
    lines.append('')

    lines.append('OUTCOME DISTRIBUTION')
    lines.append('')
    for k, v in outcome_counts.most_common():
        lines.append(f'  {k:<20} {v:>10,}  ({v/total*100:.2f}%)')
    empty_outcome = total - outcome_total
    lines.append(f'  {"(no outcome)":<20} {empty_outcome:>10,}  ({empty_outcome/total*100:.2f}%)')
    lines.append('')

    lines.append('SOURCE DISTRIBUTION')
    lines.append('')
    for k, v in source_counts.most_common():
        lines.append(f'  {k:<20} {v:>10,}')
    lines.append('')

    if suspicious_vlj:
        lines.append('SUSPICIOUS VLJ NAMES (first 30)')
        lines.append('')
        for fname, vlj in suspicious_vlj[:30]:
            lines.append(f'  {fname}: "{vlj}"')
        lines.append('')

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    log(f'QA report written: {REPORT_PATH}')

    # Pass 2: stratified sample generation
    sample = []
    for ttype, rows in by_type.items():
        random.shuffle(rows)
        taken = rows[:SAMPLE_PER_TYPE]
        sample.extend(taken)
        log(f'  {ttype}: {len(taken)} of {len(rows):,} sampled')

    random.shuffle(sample)

    review_fields = ['reviewer_correct', 'reviewer_note', 'reviewer_initials']
    if sample:
        all_fields = list(sample[0].keys()) + review_fields
        with open(OUTPUT_SAMPLE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction='ignore')
            writer.writeheader()
            for row in sample:
                row.update({k: '' for k in review_fields})
                writer.writerow(row)

    log(f'QA sample written: {OUTPUT_SAMPLE} ({len(sample)} rows)')
    log('')
    log('REVIEWER INSTRUCTIONS:')
    log('  For each row:')
    log('  1. Open file_path in a text editor')
    log('  2. Verify vlj_name matches the VLJ signature block')
    log('  3. Verify attorney_name matches ATTORNEY FOR THE BOARD')
    log('  4. Verify representation matches Appellant represented by')
    log('  5. Verify issues match THE ISSUES section')
    log('  6. Verify outcome matches ORDER/REMAND disposition')
    log('  7. Set reviewer_correct = 1 if all fields correct, 0 if any wrong')
    log('')
    log(f'After review, save as: {RESULTS_CSV}')

    with open(LOG_PATH, 'w', encoding='utf-8') as lf:
        lf.write('\n'.join(log_lines))


if __name__ == '__main__':
    main()
