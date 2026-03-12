# 01_template_classifier.py
# Stage 1: Template Classifier
# Reads stage0_profile.csv, assigns template type to each file.
# No file I/O: operates on profile data only.

import csv
import os
import datetime
_ROOT = os.path.dirname(os.path.abspath(__file__))


# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV  = os.path.join(_ROOT, "stage0_profile.csv")
OUTPUT_CSV = os.path.join(_ROOT, "stage1_classifications.csv")
LOG_PATH   = os.path.join(_ROOT, "stage1_classifier.log")
# ─────────────────────────────────────────────────────────────────────────────


def classify(row):
    """
    Apply classification rules to a Stage 0 profile row.
    Returns (template_type, confidence, note).
    """
    cit     = row['citation_type']          # AMA / LEGACY / UNKNOWN
    o_hdr   = row['order_in_header'] == '1'
    o_ftr   = row['order_in_footer'] == '1'
    o_tot   = int(row['order_count_total'] or 0)
    rem     = row['remanded_present'] == '1'
    issues  = row['issues_block_present'] == '1'
    has_err = bool(row['error'])

    # Hard error: cannot classify
    if has_err and not row['char_count']:
        return 'UNCLASSIFIED', '-', f'read_error:{row["error"]}'

    # Priority 1: Clean AMA, ORDER at top, no remand
    if cit == 'AMA' and o_hdr and not rem:
        return 'AMA_ORDER_TOP', 'HIGH', 'AMA+order_header+no_remand'

    # Priority 2: AMA with both ORDER and REMANDED
    if cit == 'AMA' and o_hdr and rem:
        return 'MIXED', 'HIGH', 'AMA+order_header+remanded'

    # Priority 3: Clean legacy, ORDER at bottom, ISSUES at top, no remand
    if cit == 'LEGACY' and o_ftr and issues and not rem:
        return 'LEGACY_ORDER_BOTTOM', 'HIGH', 'LEGACY+order_footer+issues+no_remand'

    # Priority 4: Legacy with both ORDER and REMANDED
    if cit == 'LEGACY' and o_ftr and issues and rem:
        return 'MIXED', 'HIGH', 'LEGACY+order_footer+issues+remanded'

    # Priority 5: No ORDER anywhere but REMANDED present
    if not o_hdr and not o_ftr and rem:
        return 'REMAND_ONLY', 'HIGH', 'no_order+remanded'

    # Priority 6: AMA but no ORDER in header - anomalous
    if cit == 'AMA' and not o_hdr:
        note = f'AMA+no_order_header (order_ftr={int(o_ftr)}, remanded={int(rem)}, order_count={o_tot})'
        return 'AMA_ANOMALY', 'LOW', note

    # Priority 7: Legacy but ORDER not in footer
    if cit == 'LEGACY' and not o_ftr:
        note = f'LEGACY+no_order_footer (order_hdr={int(o_hdr)}, issues={int(issues)}, order_count={o_tot})'
        return 'LEGACY_ANOMALY', 'LOW', note

    # Priority 8: Legacy with ORDER in footer but missing ISSUES block
    if cit == 'LEGACY' and o_ftr and not issues:
        return 'LEGACY_ORDER_BOTTOM', 'LOW', 'LEGACY+order_footer+NO_issues_block'

    # Priority 9: Unknown citation type with any ORDER signal
    if cit == 'UNKNOWN' and (o_hdr or o_ftr):
        note = f'UNKNOWN_citation+order_hdr={int(o_hdr)}_ftr={int(o_ftr)}'
        return 'UNCLASSIFIED', '-', note

    # Catch-all
    note = (f'no_rule_matched: cit={cit} o_hdr={int(o_hdr)} o_ftr={int(o_ftr)} '
            f'rem={int(rem)} issues={int(issues)} o_tot={o_tot}')
    return 'UNCLASSIFIED', '-', note


def main():
    os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

    log_lines = []
    def log(msg):
        ts   = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'{ts} | {msg}'
        log_lines.append(line)
        print(line, flush=True)

    log('Stage 1: Template Classifier starting')

    from collections import Counter
    type_counts = Counter()
    conf_counts = Counter()
    total = 0

    fieldnames = [
        'file_path', 'file_name', 'citation_nr', 'citation_type',
        'template_type', 'confidence',
        'signal_citation', 'signal_order_hdr', 'signal_order_ftr',
        'signal_remanded', 'signal_issues',
        'classification_note'
    ]

    with open(INPUT_CSV,  'r', encoding='utf-8') as fin, \
         open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            ttype, conf, note = classify(row)
            type_counts[ttype] += 1
            conf_counts[conf]  += 1

            writer.writerow({
                'file_path':           row['file_path'],
                'file_name':           row['file_name'],
                'citation_nr':         row['citation_nr'],
                'citation_type':       row['citation_type'],
                'template_type':       ttype,
                'confidence':          conf,
                'signal_citation':     row['citation_type'],
                'signal_order_hdr':    row['order_in_header'],
                'signal_order_ftr':    row['order_in_footer'],
                'signal_remanded':     row['remanded_present'],
                'signal_issues':       row['issues_block_present'],
                'classification_note': note,
            })

            if total % 50000 == 0:
                log(f'Classified: {total:,}')

    log(f'DONE: Total: {total:,}')
    log('Template type distribution:')
    for k, v in type_counts.most_common():
        log(f'  {k:<30} {v:>10,}  ({v/total*100:.2f}%)')
    log('Confidence distribution:')
    for k, v in conf_counts.most_common():
        log(f'  {k:<10} {v:>10,}')

    with open(LOG_PATH, 'w', encoding='utf-8') as lf:
        lf.write('\n'.join(log_lines))


if __name__ == '__main__':
    main()
