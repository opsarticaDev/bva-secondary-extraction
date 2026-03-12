# 01_validate_stage1.py
# Review Stage 1 distribution and flag anomalies before writing Stage 2 extractors.

import csv
from collections import Counter
_ROOT = os.path.dirname(os.path.abspath(__file__))

CSV_PATH = os.path.join(_ROOT, "stage1_classifications.csv")

types = Counter()
low_conf = []
total = 0

with open(CSV_PATH, 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        total += 1
        types[row['template_type']] += 1
        if row['confidence'] == 'LOW':
            low_conf.append(row)

print(f"\nTotal: {total:,}")
print(f"\nTemplate type distribution:")
for k, v in types.most_common():
    print(f"  {k:<30} {v:>10,}  ({v/total*100:.2f}%)")

unclassified = types.get('UNCLASSIFIED', 0) + types.get('AMA_ANOMALY', 0) + types.get('LEGACY_ANOMALY', 0)
print(f"\nTotal requiring investigation: {unclassified:,} ({unclassified/total*100:.2f}%)")
print(f"LOW confidence: {len(low_conf):,}")

print(f"\nFirst 20 LOW confidence records:")
for r in low_conf[:20]:
    print(f"  {r['file_name']:<30} {r['template_type']:<25} {r['classification_note']}")
