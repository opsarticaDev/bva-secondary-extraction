# 00_validate_stage0.py
# Quick validation of Stage 0 output. Run before Stage 1.

import csv
from collections import Counter
_ROOT = os.path.dirname(os.path.abspath(__file__))

CSV_PATH = os.path.join(_ROOT, "stage0_profile.csv")

citation_types  = Counter()
order_patterns  = Counter()
remanded_counts = Counter()
error_count     = 0
total           = 0

with open(CSV_PATH, 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        total += 1
        citation_types[row['citation_type']] += 1

        if row['error']:
            error_count += 1

        key = (
            row['order_in_header'],
            row['order_in_footer'],
            row['remanded_present'],
            row['issues_block_present']
        )
        order_patterns[key] += 1
        remanded_counts[row['remanded_present']] += 1

print(f"\nTotal files profiled : {total:,}")
print(f"Errors               : {error_count:,}")
print(f"\nCitation type distribution:")
for k, v in citation_types.most_common():
    print(f"  {k:<10} {v:>10,}  ({v/total*100:.1f}%)")

print(f"\nStructural pattern distribution:")
print(f"  (order_hdr, order_ftr, remanded, issues)")
for k, v in order_patterns.most_common(20):
    print(f"  {str(k):<40} {v:>10,}  ({v/total*100:.1f}%)")

print(f"\nREMANDED present: {remanded_counts.get('1', 0):,}")
print(f"REMANDED absent : {remanded_counts.get('0', 0):,}")
