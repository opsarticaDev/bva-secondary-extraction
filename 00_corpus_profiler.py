# 00_corpus_profiler.py
# Stage 0: Corpus Profiler
# Structural fingerprinting only. No condition extraction.
# Run time estimate: ~45-90 minutes on 1.19M files depending on drive speed.

import os
import re
import csv
import datetime
import time
_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── CONFIG ────────────────────────────────────────────────────────────────────
ROOT_DIRS   = [
    os.path.join(_ROOT, "..", "decisions"),
    os.path.join(_ROOT, "..", "decisions_new"),
    os.path.join(_ROOT, "..", "decisions_round3"),
]
OUTPUT_DIR  = _ROOT
RESUME      = True   # Skip files already in output (safe restart after interruption)
LOG_INTERVAL = 10000 # Print progress every N files
# ─────────────────────────────────────────────────────────────────────────────

OUTPUT_CSV  = os.path.join(OUTPUT_DIR, "stage0_profile.csv")
LOG_PATH    = os.path.join(OUTPUT_DIR, "stage0_profiler.log")

HEADER_CHARS  = 800
FOOTER_CHARS  = 3000
ISSUES_CHARS  = 2000

# Standalone ORDER: a line that IS exactly "ORDER" (with optional whitespace)
# Must not match "IN ORDER TO" or "ORDER OF" etc.
RE_ORDER_LINE      = re.compile(r'(?:^|\n)\s*ORDER\s*(?:\r?\n)', re.MULTILINE)
RE_REMANDED_LINE   = re.compile(r'(?:^|\n)\s*REMANDED\s*(?:\r?\n)', re.MULTILINE)
RE_ISSUES_LINE     = re.compile(r'THE ISSUES', re.MULTILINE)
RE_VLJ_SIGNATURE   = re.compile(
    r'(Veterans\s+Law\s+Judge|Acting\s+Veterans\s+Law\s+Judge)',
    re.IGNORECASE | re.MULTILINE
)
RE_YOUR_RIGHTS     = re.compile(r'YOUR RIGHTS TO APPEAL', re.IGNORECASE | re.MULTILINE)
RE_CITATION_AMA    = re.compile(r'Citation\s+Nr[:\s]+([A-Za-z]\d{5,10})', re.IGNORECASE)
RE_CITATION_LEGACY = re.compile(r'Citation\s+Nr[:\s]+(\d{5,10})', re.IGNORECASE)


def read_file(path):
    """Read file, try UTF-16 BOM then UTF-8 then CP1252. Return (text, encoding, error)."""
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
                return text, enc, ''
    except Exception:
        pass
    for enc in ('utf-8', 'cp1252'):
        try:
            with open(path, 'r', encoding=enc, errors='strict') as f:
                return f.read(), enc, ''
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            return '', 'unknown', str(e)
    # Fallback: read with replacement
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            return f.read(), 'utf-8-replace', 'encoding_fallback'
    except Exception as e:
        return '', 'unknown', str(e)


def extract_citation(text_head):
    """Extract citation nr from first 500 chars. Return (nr, type)."""
    snippet = text_head[:500]
    m = RE_CITATION_AMA.search(snippet)
    if m:
        return m.group(1), 'AMA'
    m = RE_CITATION_LEGACY.search(snippet)
    if m:
        return m.group(1), 'LEGACY'
    return '', 'UNKNOWN'


def profile_file(path):
    """Return dict of structural features for one file."""
    row = {
        'file_name': os.path.basename(path),
        'file_path': path,
        'file_size_bytes': '',
        'encoding': '',
        'char_count': '',
        'citation_nr': '',
        'citation_type': '',
        'order_in_header': '',
        'order_in_footer': '',
        'order_count_total': '',
        'remanded_present': '',
        'issues_block_present': '',
        'vlj_signature_present': '',
        'your_rights_present': '',
        'error': '',
    }

    try:
        row['file_size_bytes'] = os.path.getsize(path)
    except Exception as e:
        row['error'] = f'stat:{e}'
        return row

    text, enc, err = read_file(path)
    row['encoding'] = enc
    row['error']    = err

    if not text:
        return row

    row['char_count'] = len(text)

    header = text[:HEADER_CHARS]
    footer = text[-FOOTER_CHARS:] if len(text) > FOOTER_CHARS else text

    # Citation
    row['citation_nr'], row['citation_type'] = extract_citation(text)

    # ORDER presence
    all_order = RE_ORDER_LINE.findall(text)
    row['order_count_total'] = len(all_order)
    row['order_in_header']   = 1 if RE_ORDER_LINE.search(header) else 0
    row['order_in_footer']   = 1 if RE_ORDER_LINE.search(footer) else 0

    # REMANDED presence
    row['remanded_present']  = 1 if RE_REMANDED_LINE.search(text) else 0

    # THE ISSUES in first 2000 chars
    row['issues_block_present'] = 1 if RE_ISSUES_LINE.search(text[:ISSUES_CHARS]) else 0

    # VLJ signature
    row['vlj_signature_present'] = 1 if RE_VLJ_SIGNATURE.search(text) else 0

    # YOUR RIGHTS TO APPEAL
    row['your_rights_present'] = 1 if RE_YOUR_RIGHTS.search(text) else 0

    return row


def discover_txt_files(roots):
    """Walk multiple root directories, dedup by filename (first seen wins)."""
    seen = set()
    for root in roots:
        if not os.path.isdir(root):
            continue
        for dirpath, _, filenames in os.walk(root):
            for name in filenames:
                if name.lower().endswith('.txt'):
                    if name in seen:
                        continue
                    seen.add(name)
                    yield os.path.join(dirpath, name)


def load_done_set(csv_path):
    done = set()
    if not os.path.exists(csv_path):
        return done
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                done.add(row['file_path'])
    except Exception:
        pass
    return done


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    log_lines = []
    def log(msg):
        ts  = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f'{ts} | {msg}'
        log_lines.append(line)
        print(line, flush=True)

    log('Stage 0: Corpus Profiler starting')
    log(f'ROOT_DIRS : {ROOT_DIRS}')
    log(f'OUTPUT_CSV: {OUTPUT_CSV}')
    log(f'RESUME    : {RESUME}')

    done_set = load_done_set(OUTPUT_CSV) if RESUME else set()
    log(f'Resume: {len(done_set)} files already profiled')

    fieldnames = [
        'file_name','file_path','file_size_bytes','encoding','char_count',
        'citation_nr','citation_type',
        'order_in_header','order_in_footer','order_count_total',
        'remanded_present','issues_block_present',
        'vlj_signature_present','your_rights_present','error'
    ]

    write_header = not os.path.exists(OUTPUT_CSV) or not RESUME
    mode = 'a' if (RESUME and os.path.exists(OUTPUT_CSV)) else 'w'

    start = time.time()
    processed = 0
    skipped   = 0
    errors    = 0

    with open(OUTPUT_CSV, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()

        for path in discover_txt_files(ROOT_DIRS):
            if path in done_set:
                skipped += 1
                continue

            row = profile_file(path)
            writer.writerow(row)
            processed += 1

            if row['error']:
                errors += 1

            if processed % LOG_INTERVAL == 0:
                elapsed = time.time() - start
                rate    = processed / elapsed if elapsed > 0 else 0
                log(f'Processed: {processed:,} | Skipped: {skipped:,} | '
                    f'Errors: {errors} | Rate: {rate:.0f}/s')
                f.flush()

    elapsed = time.time() - start
    log(f'DONE: Processed: {processed:,} | Skipped: {skipped:,} | '
        f'Errors: {errors} | Elapsed: {elapsed:.0f}s')

    with open(LOG_PATH, 'w', encoding='utf-8') as lf:
        lf.write('\n'.join(log_lines))


if __name__ == '__main__':
    main()
