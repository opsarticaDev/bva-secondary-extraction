# 05b_qa_report.py
# Stage 5b: QA Report
# Run after reviewer fills in stage5_qa_results.csv

import csv
from collections import Counter, defaultdict
_ROOT = os.path.dirname(os.path.abspath(__file__))

RESULTS_CSV = os.path.join(_ROOT, "stage5_qa_results.csv")

by_type = defaultdict(list)
total_correct = 0
total_wrong   = 0

with open(RESULTS_CSV, 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        correct = row.get('reviewer_correct', '').strip()
        if correct not in ('0', '1'):
            continue
        ttype = row.get('template_type', 'UNKNOWN')
        by_type[ttype].append(int(correct))
        if int(correct) == 1:
            total_correct += 1
        else:
            total_wrong += 1

print('\nQA VALIDATION REPORT')
print('=' * 50)

all_pass = True
for ttype, scores in sorted(by_type.items()):
    n        = len(scores)
    correct  = sum(scores)
    accuracy = correct / n * 100 if n else 0
    status   = 'PASS' if accuracy >= 98 else 'FAIL: NEEDS REVISION'
    print(f'  {ttype:<30} {correct}/{n} ({accuracy:.1f}%)  {status}')
    if accuracy < 98:
        all_pass = False

total = total_correct + total_wrong
overall = total_correct / total * 100 if total else 0
print(f'\n  OVERALL: {total_correct}/{total} ({overall:.1f}%)')

if all_pass:
    print('\n  ALL TYPES PASS: Pipeline ready for full corpus run')
else:
    print('\n  FAILURES FOUND: Fix indicated extractor(s) before full corpus run')
