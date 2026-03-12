---
title: "Stage 3 Investigation Findings"
tags: [bva, pipeline]
type: note
project: BVA Project
status: active
created: 2026-03-11
modified: 2026-03-11
---
# Stage 3: UNCLASSIFIED Investigation Findings

## Summary

679,970 files flagged across three anomaly types (57.5% of 1,181,435 corpus).

| Type | Count | Root Cause |
|------|-------|------------|
| LEGACY_ANOMALY | 379,526 | Legacy format without ORDER footer section. Mostly remand-only decisions. |
| UNCLASSIFIED | 299,541 | UTF-16 encoded files (99.5% confirmed from 200-file sample). read_file() misses UTF-16 BOM. |
| AMA_ANOMALY | 903 | AMA format without ORDER header. |

## UNCLASSIFIED: UTF-16 Encoding

All 299,541 UNCLASSIFIED files have citation_type=UNKNOWN because the classifier read them as garbled UTF-8. The files start with the UTF-16 BOM (0xFF 0xFE). When read as UTF-8 with error replacement, every character appears space-separated ("C i t a t i o n   N r"), so no regex patterns match.

Fix: add UTF-16 BOM detection to read_file() in 01_template_classifier.py and 02_extract_fields.py. The function should check for BOM bytes before trying utf-8/cp1252.

Reclassification approach: write a targeted script (03b_reclassify_utf16.py) that re-reads and re-classifies only the UNCLASSIFIED files, then patches stage1_classifications.csv. Re-extraction for these files follows.

## LEGACY_ANOMALY: Remand-Only Decisions

All 379,526 have classification_note "LEGACY+no_order_footer". These are Legacy-format decisions with Citation Nr headers, THE ISSUES sections, REPRESENTATION, ATTORNEY FOR THE BOARD, and VLJ signatures, but no standalone ORDER section. They typically end with a REMAND section.

Stage 2 extraction should handle most fields correctly:
- VLJ: extracted from underline/bare name patterns (present in these files)
- Attorney: extracted from ATTORNEY FOR THE BOARD section (present)
- Representation: extracted from REPRESENTATION section (present)
- Issues: extracted from THE ISSUES section (present)
- Outcome: may miss these since ORDER section is absent, but REMAND_ONLY pathway catches some

Expected yield: VLJ, attorney, rep, issues near 99%. Outcome detection should produce "remanded" via the REMAND section detector, not via ORDER section parsing.

## AMA_ANOMALY: 903 Files

Too small to investigate separately. These are AMA-format files without the expected ORDER header. Likely edge cases in the AMA template (withdrawn appeals, procedural orders). Stage 2 will attempt extraction regardless.

## Action Items

1. Fix read_file() to add UTF-16 detection (both scripts)
2. Write 03b_reclassify_utf16.py for targeted reclassification of 299,541 files
3. After Stage 2 completes on current run, assess extraction rates on LEGACY_ANOMALY
4. Run targeted re-extraction on reclassified UTF-16 files
5. Merge all results into final stage2_extractions.csv
