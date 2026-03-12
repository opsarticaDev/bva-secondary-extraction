# 02_extract_fields.py
# Stage 2: Field Extraction
# Extracts judge, attorney, representation, issues, and outcome from each decision.
# Runs on ALL classified files. Template type informs extraction strategy but
# extraction is attempted regardless. The data is what we have.

import csv
import os
import re
import datetime
import time
_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── CONFIG ────────────────────────────────────────────────────────────────────
INPUT_CSV  = os.path.join(_ROOT, "stage1_classifications.csv")
OUTPUT_CSV = os.path.join(_ROOT, "stage2_extractions.csv")
LOG_PATH   = os.path.join(_ROOT, "stage2_extractor.log")
LOG_INTERVAL = 10000
# ─────────────────────────────────────────────────────────────────────────────

# Regex patterns

# VLJ: underline block or bare name before "Veterans Law Judge"
RE_VLJ_UNDERLINE = re.compile(
    r'_{5,}\s*\n\s*(.+?)\s*\n.*?Veterans\s+Law\s+Judge',
    re.IGNORECASE | re.DOTALL
)
RE_VLJ_BARE = re.compile(
    r'\n([A-Z][A-Z.\s\'-]{3,40})\s*\n+\s*(?:Acting\s+)?Veterans\s+Law\s+Judge',
    re.MULTILINE
)

# Attorney
RE_ATTORNEY_LEGACY = re.compile(
    r'ATTORNEY\s+FOR\s+THE\s+BOARD\s*\n+\s*(.+)',
    re.IGNORECASE
)
RE_ATTORNEY_AMA = re.compile(
    r'Attorney\s+for\s+the\s+Board\s+(.+)',
    re.IGNORECASE
)

# Representation
RE_REPRESENTATION = re.compile(
    r'(?:Appellant|Veteran)\s+represented\s+by:\s*(.+)',
    re.IGNORECASE
)

# Issues section (Legacy): capture block between THE ISSUE(S) and REPRESENTATION
RE_ISSUES_BLOCK = re.compile(
    r'THE\s+ISSUES?\s*\n(.*?)(?:\nREPRESENTATION|\nWITNESS|\nATTORNEY\s+FOR)',
    re.IGNORECASE | re.DOTALL
)

# Individual issue lines (numbered)
RE_ISSUE_NUMBERED = re.compile(
    r'^\s*\d+\.\s+(.+?)(?:\n\n|\n\s*\d+\.|\Z)',
    re.MULTILINE | re.DOTALL
)

# Single issue (no number, just paragraph after THE ISSUE header)
RE_ISSUE_SINGLE = re.compile(
    r'THE\s+ISSUE\s*\n\s*\n\s*(.+?)(?:\n\s*\n)',
    re.IGNORECASE | re.DOTALL
)

# ORDER section
RE_ORDER_SECTION = re.compile(
    r'\nORDER\s*\n(.*?)(?:\nREMAND|\nREASONS|\n_{5,}|\nFINDINGS|\Z)',
    re.IGNORECASE | re.DOTALL
)

# AMA ORDER at top (after DOCKET/DATE lines)
RE_ORDER_TOP = re.compile(
    r'\nORDER\s*\n(.*?)(?:\nFINDINGS\s+OF\s+FACT)',
    re.IGNORECASE | re.DOTALL
)

# Outcome per line in ORDER
RE_GRANTED = re.compile(r'\bis\s+granted\b', re.IGNORECASE)
RE_DENIED = re.compile(r'\bis\s+denied\b', re.IGNORECASE)

# REMAND section presence
RE_REMAND_SECTION = re.compile(r'\nREMAND\s*\n', re.IGNORECASE)


def read_file(path):
    # Check for UTF-16 BOM first
    try:
        with open(path, 'rb') as bf:
            bom = bf.read(2)
        if bom == b'\xff\xfe' or bom == b'\xfe\xff':
            enc = 'utf-16-le' if bom == b'\xff\xfe' else 'utf-16-be'
            with open(path, 'r', encoding=enc, errors='replace') as f:
                text = f.read()
                if text and text[0] == '\ufeff':
                    text = text[1:]
                return text, ''
    except Exception:
        pass
    for enc in ('utf-8', 'cp1252'):
        try:
            with open(path, 'r', encoding=enc, errors='strict') as f:
                return f.read(), ''
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            return '', str(e)
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read(), 'encoding_fallback'
    except Exception as e:
        return '', str(e)


def clean(s):
    """Collapse whitespace, strip."""
    if not s:
        return ''
    return re.sub(r'\s+', ' ', s).strip()


def extract_vlj(text):
    m = RE_VLJ_UNDERLINE.search(text)
    if m:
        name = clean(m.group(1))
        if name and len(name) > 2 and name.upper() != 'BOARD':
            return name
    m = RE_VLJ_BARE.search(text)
    if m:
        name = clean(m.group(1))
        if name and len(name) > 2:
            return name
    return ''


def extract_attorney(text):
    m = RE_ATTORNEY_LEGACY.search(text)
    if m:
        return clean(m.group(1))
    m = RE_ATTORNEY_AMA.search(text)
    if m:
        return clean(m.group(1))
    return ''


def extract_representation(text):
    m = RE_REPRESENTATION.search(text)
    if m:
        return clean(m.group(1))
    return ''


def extract_issues(text, template_type):
    issues = []

    if template_type.startswith('AMA'):
        # AMA: issues are in ORDER section at top
        m = RE_ORDER_TOP.search(text)
        if m:
            block = m.group(1)
            # Each paragraph in the ORDER block is one issue+outcome
            paragraphs = re.split(r'\n\s*\n', block.strip())
            for p in paragraphs:
                p = clean(p)
                if p and len(p) > 10:
                    issues.append(p)
            if issues:
                return issues

    # Legacy / fallback: THE ISSUE(S) section
    m = RE_ISSUES_BLOCK.search(text)
    if m:
        block = m.group(1)
        # Try numbered issues first
        numbered = RE_ISSUE_NUMBERED.findall(block)
        if numbered:
            for iss in numbered:
                c = clean(iss)
                if c and len(c) > 5:
                    issues.append(c)
            if issues:
                return issues
        # Single issue (no number)
        single = clean(block)
        if single and len(single) > 5:
            return [single]

    # Last resort: single issue pattern
    m = RE_ISSUE_SINGLE.search(text)
    if m:
        c = clean(m.group(1))
        if c and len(c) > 5:
            return [c]

    return issues


def extract_outcome(text, template_type):
    """Return (outcome_summary, outcome_details)."""
    granted = 0
    denied = 0
    remanded = 0

    # Find ORDER section
    if template_type.startswith('AMA'):
        m = RE_ORDER_TOP.search(text)
    else:
        m = RE_ORDER_SECTION.search(text)

    if m:
        block = m.group(1)
        lines = block.split('\n')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            if RE_GRANTED.search(line):
                granted += 1
            if RE_DENIED.search(line):
                denied += 1

    # Check for REMAND section
    if RE_REMAND_SECTION.search(text):
        remanded = 1

    # Also check for "REMANDED" in the ORDER text
    if m:
        block_lower = m.group(1).lower()
        if 'remand' in block_lower:
            remanded = 1

    if granted == 0 and denied == 0 and remanded == 0:
        # REMAND_ONLY type: entire decision is a remand
        if template_type == 'REMAND_ONLY':
            return 'remanded', 'remand_only'
        return '', 'no_order_found'

    parts = []
    if granted:
        parts.append(f'{granted}_granted')
    if denied:
        parts.append(f'{denied}_denied')
    if remanded:
        parts.append('remanded')
    detail = '+'.join(parts)

    if granted > 0 and denied == 0 and remanded == 0:
        return 'granted', detail
    if denied > 0 and granted == 0 and remanded == 0:
        return 'denied', detail
    if remanded and granted == 0 and denied == 0:
        return 'remanded', detail
    return 'mixed', detail


def main():
    log_lines = []
    def log(msg):
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'{ts} | {msg}'
        log_lines.append(line)
        print(line, flush=True)

    # Resume support: if output CSV exists, count rows and skip that many
    resume_from = 0
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, 'r', encoding='utf-8') as check:
            resume_from = sum(1 for _ in check) - 1  # subtract header
        if resume_from > 0:
            log(f'Resuming from row {resume_from:,}')

    fieldnames = [
        'file_path', 'file_name', 'citation_nr', 'citation_type',
        'template_type', 'confidence',
        'vlj_name', 'attorney_name', 'representation',
        'issue_count', 'issues',
        'outcome', 'outcome_detail',
        'error'
    ]

    from collections import Counter
    outcome_counts = Counter()
    vlj_found = 0
    attorney_found = 0
    rep_found = 0
    issues_found = 0
    total = 0
    skipped = 0
    errors = 0

    start = time.time()

    mode = 'a' if resume_from > 0 else 'w'
    with open(INPUT_CSV, 'r', encoding='utf-8') as fin, \
         open(OUTPUT_CSV, mode, newline='', encoding='utf-8') as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        if mode == 'w':
            writer.writeheader()

        for row in reader:
            if skipped < resume_from:
                skipped += 1
                continue
            total += 1
            fpath = row['file_path']
            ttype = row['template_type']

            text, err = read_file(fpath)
            if err and not text:
                errors += 1
                writer.writerow({
                    'file_path': fpath,
                    'file_name': row['file_name'],
                    'citation_nr': row['citation_nr'],
                    'citation_type': row['citation_type'],
                    'template_type': ttype,
                    'confidence': row['confidence'],
                    'vlj_name': '', 'attorney_name': '', 'representation': '',
                    'issue_count': 0, 'issues': '',
                    'outcome': '', 'outcome_detail': '',
                    'error': f'read:{err}'
                })
                continue

            vlj = extract_vlj(text)
            attorney = extract_attorney(text)
            rep = extract_representation(text)
            issues = extract_issues(text, ttype)
            outcome, detail = extract_outcome(text, ttype)

            if vlj:
                vlj_found += 1
            if attorney:
                attorney_found += 1
            if rep:
                rep_found += 1
            if issues:
                issues_found += 1
            outcome_counts[outcome] += 1

            writer.writerow({
                'file_path': fpath,
                'file_name': row['file_name'],
                'citation_nr': row['citation_nr'],
                'citation_type': row['citation_type'],
                'template_type': ttype,
                'confidence': row['confidence'],
                'vlj_name': vlj,
                'attorney_name': attorney,
                'representation': rep,
                'issue_count': len(issues),
                'issues': ' | '.join(issues),
                'outcome': outcome,
                'outcome_detail': detail,
                'error': err
            })

            if total % LOG_INTERVAL == 0:
                elapsed = time.time() - start
                rate = total / elapsed if elapsed > 0 else 0
                log(f'Processed: {total:,} | Rate: {rate:.0f}/s | '
                    f'VLJ: {vlj_found:,} | Atty: {attorney_found:,} | '
                    f'Rep: {rep_found:,} | Issues: {issues_found:,} | '
                    f'Errors: {errors}')
                fout.flush()

    elapsed = time.time() - start
    log(f'DONE: Total: {total:,} | Elapsed: {elapsed:.0f}s | Errors: {errors}')
    log(f'VLJ found: {vlj_found:,} ({vlj_found/total*100:.1f}%)')
    log(f'Attorney found: {attorney_found:,} ({attorney_found/total*100:.1f}%)')
    log(f'Representation found: {rep_found:,} ({rep_found/total*100:.1f}%)')
    log(f'Issues found: {issues_found:,} ({issues_found/total*100:.1f}%)')
    log(f'Outcome distribution:')
    for k, v in outcome_counts.most_common():
        label = k if k else '(empty)'
        log(f'  {label:<20} {v:>10,}  ({v/total*100:.2f}%)')

    with open(LOG_PATH, 'w', encoding='utf-8') as lf:
        lf.write('\n'.join(log_lines))


if __name__ == '__main__':
    main()
