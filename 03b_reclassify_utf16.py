# 03b_reclassify_utf16.py
# Stage 3b: UTF-16 Reclassification and Extraction
# Targeted pass: re-reads UNCLASSIFIED files with UTF-16 support,
# re-profiles, re-classifies, and extracts fields in one pass.
# Outputs patched stage1 and stage2 CSVs for the affected files.

import csv
import os
import re
import datetime
import time
_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── CONFIG ────────────────────────────────────────────────────────────────────
STAGE1_CSV      = os.path.join(_ROOT, "stage1_classifications.csv")
OUTPUT_STAGE1   = os.path.join(_ROOT, "stage1b_reclassified.csv")
OUTPUT_STAGE2   = os.path.join(_ROOT, "stage2b_extractions.csv")
LOG_PATH        = os.path.join(_ROOT, "stage3b_reclassify.log")
LOG_INTERVAL    = 10000
# ─────────────────────────────────────────────────────────────────────────────

# === Profile signals (from Stage 0) ===
HEADER_CHARS = 800
FOOTER_CHARS = 3000

RE_ORDER_LINE    = re.compile(r'(?:^|\n)\s*ORDER\s*(?:\r?\n)', re.MULTILINE)
RE_REMANDED_LINE = re.compile(r'(?:^|\n)\s*REMANDED\s*(?:\r?\n)', re.MULTILINE)
RE_ISSUES_LINE   = re.compile(r'THE ISSUES', re.MULTILINE)
RE_CITATION_AMA  = re.compile(r'Citation\s+Nr[:\s]+([A-Za-z]\d{5,10})', re.IGNORECASE)
RE_CITATION_LEGACY = re.compile(r'Citation\s+Nr[:\s]+(\d{5,10})', re.IGNORECASE)

# === Extraction patterns (from Stage 2) ===
RE_VLJ_UNDERLINE = re.compile(
    r'_{5,}\s*\n\s*(.+?)\s*\n.*?Veterans\s+Law\s+Judge',
    re.IGNORECASE | re.DOTALL
)
RE_VLJ_BARE = re.compile(
    r'\n([A-Z][A-Z.\s\'-]{3,40})\s*\n+\s*(?:Acting\s+)?Veterans\s+Law\s+Judge',
    re.MULTILINE
)
RE_ATTORNEY_LEGACY = re.compile(
    r'ATTORNEY\s+FOR\s+THE\s+BOARD\s*\n+\s*(.+)', re.IGNORECASE
)
RE_ATTORNEY_AMA = re.compile(
    r'Attorney\s+for\s+the\s+Board\s+(.+)', re.IGNORECASE
)
RE_REPRESENTATION = re.compile(
    r'(?:Appellant|Veteran)\s+represented\s+by:\s*(.+)', re.IGNORECASE
)
RE_ISSUES_BLOCK = re.compile(
    r'THE\s+ISSUES?\s*\n(.*?)(?:\nREPRESENTATION|\nWITNESS|\nATTORNEY\s+FOR)',
    re.IGNORECASE | re.DOTALL
)
RE_ISSUE_NUMBERED = re.compile(
    r'^\s*\d+\.\s+(.+?)(?:\n\n|\n\s*\d+\.|\Z)',
    re.MULTILINE | re.DOTALL
)
RE_ISSUE_SINGLE = re.compile(
    r'THE\s+ISSUE\s*\n\s*\n\s*(.+?)(?:\n\s*\n)',
    re.IGNORECASE | re.DOTALL
)
RE_ORDER_SECTION = re.compile(
    r'\nORDER\s*\n(.*?)(?:\nREMAND|\nREASONS|\n_{5,}|\nFINDINGS|\Z)',
    re.IGNORECASE | re.DOTALL
)
RE_ORDER_TOP = re.compile(
    r'\nORDER\s*\n(.*?)(?:\nFINDINGS\s+OF\s+FACT)',
    re.IGNORECASE | re.DOTALL
)
RE_GRANTED = re.compile(r'\bis\s+granted\b', re.IGNORECASE)
RE_DENIED = re.compile(r'\bis\s+denied\b', re.IGNORECASE)
RE_REMAND_SECTION = re.compile(r'\nREMAND\s*\n', re.IGNORECASE)


def read_file_utf16(path):
    """Read file with UTF-16 BOM detection, then UTF-8/CP1252 fallback."""
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
    if not s:
        return ''
    return re.sub(r'\s+', ' ', s).strip()


def profile_text(text):
    """Run Stage 0 profiling signals on text. Returns dict of signals."""
    header = text[:HEADER_CHARS]
    footer = text[-FOOTER_CHARS:] if len(text) > FOOTER_CHARS else text

    m_ama = RE_CITATION_AMA.search(header)
    m_leg = RE_CITATION_LEGACY.search(header)
    if m_ama:
        citation_type = 'AMA'
        citation_nr = m_ama.group(1)
    elif m_leg:
        citation_type = 'LEGACY'
        citation_nr = m_leg.group(1)
    else:
        citation_type = 'UNKNOWN'
        citation_nr = ''

    order_count = len(RE_ORDER_LINE.findall(text))
    order_in_header = 1 if RE_ORDER_LINE.search(header) else 0
    order_in_footer = 1 if RE_ORDER_LINE.search(footer) else 0
    remanded_present = 1 if RE_REMANDED_LINE.search(text) or RE_REMAND_SECTION.search(text) else 0
    issues_block = 1 if RE_ISSUES_LINE.search(text) else 0

    return {
        'citation_type': citation_type,
        'citation_nr': citation_nr,
        'order_in_header': order_in_header,
        'order_in_footer': order_in_footer,
        'order_count_total': order_count,
        'remanded_present': remanded_present,
        'issues_block_present': issues_block,
    }


def classify(profile):
    """Stage 1 classification logic. Returns (template_type, confidence, note)."""
    cit = profile['citation_type']
    o_hdr = profile['order_in_header'] == 1
    o_ftr = profile['order_in_footer'] == 1
    o_tot = profile['order_count_total']
    rem = profile['remanded_present'] == 1
    issues = profile['issues_block_present'] == 1

    if cit == 'AMA' and o_hdr and not rem:
        return 'AMA_ORDER_TOP', 'HIGH', 'AMA+order_header+no_remand'
    if cit == 'AMA' and o_hdr and rem:
        return 'MIXED', 'HIGH', 'AMA+order_header+remanded'
    if cit == 'LEGACY' and o_ftr and issues and not rem:
        return 'LEGACY_ORDER_BOTTOM', 'HIGH', 'LEGACY+order_footer+issues+no_remand'
    if cit == 'LEGACY' and o_ftr and issues and rem:
        return 'MIXED', 'HIGH', 'LEGACY+order_footer+issues+remanded'
    if not o_hdr and not o_ftr and rem:
        return 'REMAND_ONLY', 'HIGH', 'no_order+remanded'
    if cit == 'AMA' and not o_hdr:
        note = f'AMA+no_order_header (order_ftr={int(o_ftr)}, remanded={int(rem)}, order_count={o_tot})'
        return 'AMA_ANOMALY', 'LOW', note
    if cit == 'LEGACY' and not o_ftr:
        note = f'LEGACY+no_order_footer (order_hdr={int(o_hdr)}, issues={int(issues)}, order_count={o_tot})'
        return 'LEGACY_ANOMALY', 'LOW', note
    if cit == 'LEGACY' and o_ftr and not issues:
        return 'LEGACY_ORDER_BOTTOM', 'LOW', 'LEGACY+order_footer+NO_issues_block'
    if cit == 'UNKNOWN' and (o_hdr or o_ftr):
        note = f'UNKNOWN_citation+order_hdr={int(o_hdr)}_ftr={int(o_ftr)}'
        return 'UNCLASSIFIED', '-', note

    note = (f'no_rule_matched: cit={cit} o_hdr={int(o_hdr)} o_ftr={int(o_ftr)} '
            f'rem={int(rem)} issues={int(issues)} o_tot={o_tot}')
    return 'UNCLASSIFIED', '-', note


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
        m = RE_ORDER_TOP.search(text)
        if m:
            block = m.group(1)
            paragraphs = re.split(r'\n\s*\n', block.strip())
            for p in paragraphs:
                p = clean(p)
                if p and len(p) > 10:
                    issues.append(p)
            if issues:
                return issues
    m = RE_ISSUES_BLOCK.search(text)
    if m:
        block = m.group(1)
        numbered = RE_ISSUE_NUMBERED.findall(block)
        if numbered:
            for iss in numbered:
                c = clean(iss)
                if c and len(c) > 5:
                    issues.append(c)
            if issues:
                return issues
        single = clean(block)
        if single and len(single) > 5:
            return [single]
    m = RE_ISSUE_SINGLE.search(text)
    if m:
        c = clean(m.group(1))
        if c and len(c) > 5:
            return [c]
    return issues


def extract_outcome(text, template_type):
    granted = 0
    denied = 0
    remanded = 0
    if template_type.startswith('AMA'):
        m = RE_ORDER_TOP.search(text)
    else:
        m = RE_ORDER_SECTION.search(text)
    if m:
        block = m.group(1)
        for line in block.split('\n'):
            line = line.strip()
            if not line:
                continue
            if RE_GRANTED.search(line):
                granted += 1
            if RE_DENIED.search(line):
                denied += 1
    if RE_REMAND_SECTION.search(text):
        remanded = 1
    if m:
        block_lower = m.group(1).lower()
        if 'remand' in block_lower:
            remanded = 1
    if granted == 0 and denied == 0 and remanded == 0:
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

    log('Stage 3b: UTF-16 Reclassification + Extraction starting')

    # Gather UNCLASSIFIED files from Stage 1
    targets = []
    with open(STAGE1_CSV, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row['template_type'] == 'UNCLASSIFIED':
                targets.append(row)

    log(f'Found {len(targets):,} UNCLASSIFIED files to reprocess')

    from collections import Counter
    type_counts = Counter()
    outcome_counts = Counter()
    vlj_found = 0
    atty_found = 0
    rep_found = 0
    issues_found = 0
    total = 0
    errors = 0

    stage1_fields = [
        'file_path', 'file_name', 'citation_nr', 'citation_type',
        'template_type', 'confidence',
        'signal_citation', 'signal_order_hdr', 'signal_order_ftr',
        'signal_remanded', 'signal_issues',
        'classification_note'
    ]
    stage2_fields = [
        'file_path', 'file_name', 'citation_nr', 'citation_type',
        'template_type', 'confidence',
        'vlj_name', 'attorney_name', 'representation',
        'issue_count', 'issues',
        'outcome', 'outcome_detail',
        'error'
    ]

    start = time.time()

    with open(OUTPUT_STAGE1, 'w', newline='', encoding='utf-8') as f1, \
         open(OUTPUT_STAGE2, 'w', newline='', encoding='utf-8') as f2:

        w1 = csv.DictWriter(f1, fieldnames=stage1_fields)
        w2 = csv.DictWriter(f2, fieldnames=stage2_fields)
        w1.writeheader()
        w2.writeheader()

        for row in targets:
            total += 1
            fpath = row['file_path']

            text, err = read_file_utf16(fpath)
            if err and not text:
                errors += 1
                # Write original classification + empty extraction
                w1.writerow({k: row.get(k, '') for k in stage1_fields})
                w2.writerow({
                    'file_path': fpath, 'file_name': row['file_name'],
                    'citation_nr': row['citation_nr'], 'citation_type': row['citation_type'],
                    'template_type': row['template_type'], 'confidence': row['confidence'],
                    'vlj_name': '', 'attorney_name': '', 'representation': '',
                    'issue_count': 0, 'issues': '', 'outcome': '', 'outcome_detail': '',
                    'error': f'read:{err}'
                })
                continue

            # Re-profile
            profile = profile_text(text)
            ttype, conf, note = classify(profile)
            type_counts[ttype] += 1

            w1.writerow({
                'file_path': fpath,
                'file_name': row['file_name'],
                'citation_nr': profile['citation_nr'] or row['citation_nr'],
                'citation_type': profile['citation_type'],
                'template_type': ttype,
                'confidence': conf,
                'signal_citation': profile['citation_type'],
                'signal_order_hdr': profile['order_in_header'],
                'signal_order_ftr': profile['order_in_footer'],
                'signal_remanded': profile['remanded_present'],
                'signal_issues': profile['issues_block_present'],
                'classification_note': note,
            })

            # Extract fields
            vlj = extract_vlj(text)
            attorney = extract_attorney(text)
            rep = extract_representation(text)
            issues_list = extract_issues(text, ttype)
            outcome, detail = extract_outcome(text, ttype)

            if vlj:
                vlj_found += 1
            if attorney:
                atty_found += 1
            if rep:
                rep_found += 1
            if issues_list:
                issues_found += 1
            outcome_counts[outcome] += 1

            w2.writerow({
                'file_path': fpath,
                'file_name': row['file_name'],
                'citation_nr': profile['citation_nr'] or row['citation_nr'],
                'citation_type': profile['citation_type'],
                'template_type': ttype,
                'confidence': conf,
                'vlj_name': vlj,
                'attorney_name': attorney,
                'representation': rep,
                'issue_count': len(issues_list),
                'issues': ' | '.join(issues_list),
                'outcome': outcome,
                'outcome_detail': detail,
                'error': err
            })

            if total % LOG_INTERVAL == 0:
                elapsed = time.time() - start
                rate = total / elapsed if elapsed > 0 else 0
                log(f'Processed: {total:,} | Rate: {rate:.0f}/s | '
                    f'VLJ: {vlj_found:,} | Atty: {atty_found:,} | '
                    f'Rep: {rep_found:,} | Issues: {issues_found:,} | '
                    f'Errors: {errors}')
                f1.flush()
                f2.flush()

    elapsed = time.time() - start
    log(f'DONE: Total: {total:,} | Elapsed: {elapsed:.0f}s | Errors: {errors}')
    log(f'Reclassification distribution:')
    for k, v in type_counts.most_common():
        log(f'  {k:<30} {v:>10,}  ({v/total*100:.2f}%)')
    log(f'VLJ found: {vlj_found:,} ({vlj_found/total*100:.1f}%)')
    log(f'Attorney found: {atty_found:,} ({atty_found/total*100:.1f}%)')
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
