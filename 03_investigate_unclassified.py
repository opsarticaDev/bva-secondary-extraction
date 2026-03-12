# 03_investigate_unclassified.py
# Stage 3: UNCLASSIFIED Investigation
# Generates human-readable windows around structural keywords for manual review.
# Run only on UNCLASSIFIED / AMA_ANOMALY / LEGACY_ANOMALY files.

import csv
import os
import re
import random
import datetime
_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── CONFIG ────────────────────────────────────────────────────────────────────
CLASSIFICATIONS_CSV = os.path.join(_ROOT, "stage1_classifications.csv")
REPORT_PATH         = os.path.join(_ROOT, "stage3_unclassified_report.txt")
SAMPLE_CSV          = os.path.join(_ROOT, "stage3_unclassified_sample.csv")
MAX_FILES_IN_REPORT = 200    # Cap to keep report readable
SAMPLE_SIZE         = 50     # Random sample for CSV
WINDOW_CHARS        = 300    # Chars on each side of keyword occurrence
INVESTIGATE_TYPES   = {'UNCLASSIFIED', 'AMA_ANOMALY', 'LEGACY_ANOMALY'}
# ─────────────────────────────────────────────────────────────────────────────

KEYWORDS = ['ORDER', 'REMANDED', 'THE ISSUES', 'FINDINGS OF FACT', 'Citation Nr']


def read_file(path):
    for enc in ('utf-8', 'cp1252'):
        try:
            with open(path, 'r', encoding=enc, errors='strict') as f:
                return f.read()
        except Exception:
            continue
    with open(path, 'r', encoding='utf-8', errors='replace') as f:
        return f.read()


def keyword_windows(text, keyword, window=WINDOW_CHARS, max_hits=3):
    """Return up to max_hits windows around keyword occurrences."""
    windows = []
    for m in re.finditer(re.escape(keyword), text, re.IGNORECASE):
        s = max(0, m.start() - window)
        e = min(len(text), m.end() + window)
        windows.append(text[s:e].replace('\r', '').replace('\n', '↵'))
        if len(windows) >= max_hits:
            break
    return windows


def document_open(text, chars=500):
    return text[:chars].replace('\r', '').replace('\n', '↵')


def document_close(text, chars=2000):
    return text[-chars:].replace('\r', '').replace('\n', '↵')


def main():
    log_lines = []
    def log(msg):
        ts   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'{ts} | {msg}'
        log_lines.append(line)
        print(line, flush=True)

    log('Stage 3: UNCLASSIFIED Investigation starting')

    # Gather target files
    targets = []
    with open(CLASSIFICATIONS_CSV, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['template_type'] in INVESTIGATE_TYPES:
                targets.append(row)

    log(f'Found {len(targets):,} files to investigate')
    log(f'Types: { {r["template_type"] for r in targets} }')

    if not targets:
        log('Nothing to investigate. Exiting.')
        return

    # Random sample for report (cap at MAX_FILES_IN_REPORT)
    random.shuffle(targets)
    report_set = targets[:MAX_FILES_IN_REPORT]
    sample_set = targets[:SAMPLE_SIZE]

    # Write report
    lines = []
    lines.append('=' * 80)
    lines.append('BVA EXTRACTION PIPELINE: STAGE 3 UNCLASSIFIED INVESTIGATION REPORT')
    lines.append(f'Generated: {datetime.datetime.now().isoformat()}')
    lines.append(f'Total unclassified: {len(targets):,}')
    lines.append(f'Showing: {len(report_set)} files')
    lines.append('=' * 80)
    lines.append('')

    for i, row in enumerate(report_set, 1):
        path = row['file_path']
        lines.append(f'[{i}/{len(report_set)}] {row["file_name"]}')
        lines.append(f'  Type assigned: {row["template_type"]}')
        lines.append(f'  Note: {row["classification_note"]}')
        lines.append(f'  Citation: {row["citation_nr"]} ({row["citation_type"]})')

        try:
            text = read_file(path)
            lines.append(f'  Size: {len(text):,} chars')
            lines.append(f'  DOCUMENT OPEN (first 500 chars):')
            lines.append(f'    {document_open(text)}')
            lines.append(f'  DOCUMENT CLOSE (last 2000 chars):')
            lines.append(f'    {document_close(text)}')

            for kw in KEYWORDS:
                windows = keyword_windows(text, kw)
                if windows:
                    lines.append(f'  KEYWORD "{kw}" ({len(windows)} occurrence(s)):')
                    for w in windows:
                        lines.append(f'    ...{w}...')
                else:
                    lines.append(f'  KEYWORD "{kw}": NOT FOUND')

        except Exception as e:
            lines.append(f'  ERROR reading file: {e}')

        lines.append('')
        lines.append('-' * 60)
        lines.append('')

    with open(REPORT_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))

    log(f'Report written: {REPORT_PATH}')

    # Write sample CSV
    sample_fieldnames = [
        'file_name', 'file_path', 'citation_nr', 'citation_type',
        'template_type', 'classification_note'
    ]
    with open(SAMPLE_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=sample_fieldnames)
        writer.writeheader()
        for row in sample_set:
            writer.writerow({k: row.get(k, '') for k in sample_fieldnames})

    log(f'Sample CSV written: {SAMPLE_CSV} ({len(sample_set)} files)')
    log('Review the report and determine disposition for each file type.')
    log('Dispositions: NEW_VARIANT | CORRUPT | NON_DECISION | OUT_OF_SCOPE')


if __name__ == '__main__':
    main()
