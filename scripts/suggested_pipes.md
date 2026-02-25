# Suggested Additional Pipes

Based on the current scripts in this directory, here are potential pipe candidates beyond `translate`, `critics_review`, and `mosaic_orchestrator`.

## Quality and linting pipes
- **grammar_auditor**: run grammar checks and produce issue reports.
- **simile_lint_pass**: detect overused or weak similes and suggest rewrites.
- **typographic_precision_review**: enforce punctuation/quote/dash typography standards.
- **quotation_delimiter_auditor**: validate consistent quote delimiter usage.
- **html_review**: scan generated HTML for structure and formatting issues.
- **schema_validator**: validate structured outputs against expected schemas.

## Style and prose enhancement pipes
- **lexical_enhancer**: improve diction and lexical variety.
- **vivid_verb_upgrader**: replace weak verbs with more vivid alternatives.
- **slop_scrubber**: remove repetitive or low-signal phrasing.
- **prompt_transformer**: transform prompt phrasing for desired tone or constraints.

## Signal, analytics, and monitoring pipes
- **signal_density**: measure information density and flag low-signal passages.
- **surprisal_scout**: score novelty/surprisal across passages.
- **entropy_evaluator**: evaluate lexical entropy at section/document level.
- **burst_monitor**: detect burstiness trends and pacing anomalies.
- **word_frequency_benchmark**: compare token frequency against baselines.

## Editorial workflow pipes
- **pre_processing**: normalize and clean raw content before downstream steps.
- **paragraph_issue_bundle**: aggregate paragraph-level issues into one report.
- **confidence_review**: produce confidence metadata for editorial decisions.
- **theme_mapper**: map narrative themes and motif continuity.
- **direct_signal_filter**: remove or route low-priority signals before review.
- **culling_resolver**: reconcile candidate removals/culls with final output.

## Specialized utility pipes
- **lexical_entropy_amplifier**: intentionally increase lexical diversity.
- **pattern_extractor**: extract reusable rhetorical/structural patterns.
- **kokoro_paragraph_reader**: paragraph narration/audio-oriented post-processing.
- **analyzer**: combined diagnostics/meta-analysis pass.
