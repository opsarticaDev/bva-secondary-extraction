# BVA Secondary Service Connection Extraction Pipeline

Extracts secondary service connection pairs from 1.19M Board of Veterans' Appeals decisions (2010-2025). Uses structural fingerprinting and template-based classification to isolate ORDER and REMANDED blocks, then applies validated regex patterns to extract condition pairs with zero false-positive tolerance.

## Pipeline Architecture

```
Stage 0  Corpus Profiler        → stage0_profile.csv
Stage 1  Template Classifier    → stage1_classifications.csv
Stage 2  Per-Template Extractors → stage2_*.csv (written after Stage 1 review)
Stage 3  UNCLASSIFIED Investigation → stage3_unclassified_report.txt
Stage 4  Merge and Deduplication → bva_secondary_pairs.v2.raw.csv
Stage 5  QA Validation          → stage5_qa_results.csv
```

## Design Principle

Every extractor's first action is block isolation. No condition is extracted from body text, findings, conclusions, or procedural history. Extraction operates only on ORDER or REMANDED section headers, and only when a primary/secondary pair is explicitly named.

## Accuracy Target

- False negative (missed true pairs): target 0%, accept up to 5%
- False positive (wrong pairs): 0%, prefer to miss than misattribute
- Named primary required: unnamed-primary patterns are logged, never extracted

## Template Types

| Type | Description |
|------|-------------|
| AMA_ORDER_TOP | AMA citation, ORDER block in first 800 chars |
| LEGACY_ORDER_BOTTOM | Legacy citation, ORDER block near document end |
| REMAND_ONLY | No ORDER block, REMANDED block only |
| MIXED | Both ORDER and REMANDED blocks present |

## Scripts

| Script | Stage | Purpose |
|--------|-------|---------|
| 00_corpus_profiler.py | 0 | Structural fingerprint of every file |
| 00_validate_stage0.py | 0 | Validation checks before Stage 1 |
| 01_template_classifier.py | 1 | Template type assignment |
| 01_validate_stage1.py | 1 | Distribution review before Stage 2 |
| 03_investigate_unclassified.py | 3 | Human review of unclassifiable files |
| 04_merge_and_dedup.py | 4 | Combine and deduplicate Stage 2 outputs |
| 05_qa_validation.py | 5 | Stratified sample generation for review |
| 05b_qa_report.py | 5 | Accuracy report from reviewer annotations |

Stage 2 extractor scripts (02a through 02d) are written after Stage 1 results are reviewed.

## Corpus

Not included in this repository. The corpus consists of 1.19M plain text BVA decision files organized by year (2010-2025).

## Related Work

- Vernon, C. (2025). Legal Representation and Systemic Burden in the VA Disability Appeals Process. Working paper.
