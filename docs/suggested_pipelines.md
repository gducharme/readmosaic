# Suggested Pipelines

## Purpose
This document proposes a consolidated pipeline catalog based on the current ReadMosaic codebase: existing Seedpipe flows, standalone scripts, web/runtime functionality, and nearby extensions that can be implemented with minimal architectural drift.

Status legend:
- `Active`: implemented and runnable now.
- `Partial`: core pieces exist, but some stages are placeholders or manual.
- `Proposed`: not yet implemented as a full automated pipeline.

---

## 1) Translation Production Pipeline
- Status: `Partial`
- Primary goal: deterministic multi-language manuscript translation with staged artifacts.
- Current building blocks:
  - `pipes/translate/spec/phase1/pipeline.yaml`
  - `pipes/translate/src/stages/source_ingest.py`
  - `pipes/translate/src/stages/translate_pass1.py`
  - `pipes/translate/src/stages/translate_pass2.py`
  - `pipes/translate/src/stages/candidate_assembly.py`
- Suggested stage flow:
  1. source ingest (`paragraphs.jsonl`, `manifest.json`)
  2. pass 1 translation (per language)
  3. pass 2 translation/script conversion (per language)
  4. candidate manuscript assembly
  5. publish gate
- Future improvements:
  - complete placeholder review/gate stages in spec
  - parameterize language bundles per run
  - enforce schema checks after each stage

## 2) Translation Quality Gate Pipeline
- Status: `Partial`
- Primary goal: block low-quality translations before final publish.
- Current building blocks:
  - `scripts/grammar_auditor.py`
  - `scripts/typographic_precision_review.py`
  - `scripts/critics_runner.py`
  - `scripts/paragraph_issue_bundle.py`
  - `scripts/confidence_review.py`
  - `scripts/html_review.py`
- Suggested stage flow:
  1. run grammar + typography + critics reviews
  2. normalize/anchor findings to paragraph IDs
  3. aggregate scores + create rework queue
  4. render confidence and HTML reviewer views
  5. gate publish on threshold policy
- Future improvements:
  - add deterministic paragraph mapping stage in `src/stages`
  - codify scoring thresholds in versioned policy JSON
  - add regression suite with known good/bad translated paragraphs

## 3) Slop Stop Editorial Pipeline
- Status: `Active` (base), `Partial` (advanced)
- Primary goal: remove repetitive/low-signal language and improve narrative sharpness.
- Current building blocks:
  - `pipes/slop-stop-pipe/spec/phase1/pipeline.yaml`
  - `pipes/slop-stop-pipe/src/stages/preprocessing.py`
  - `pipes/slop-stop-pipe/src/stages/word_frequency_benchmark.py`
  - `pipes/slop-stop-pipe/src/stages/transform.py`
  - `scripts/slop_scrubber.py`
  - `scripts/direct_signal_filter.py`
  - `scripts/simile_lint_pass.py`
- Suggested stage flow:
  1. normalize manuscript
  2. lexical frequency benchmark
  3. style/slop detectors
  4. transformation and rewrite suggestion emit
  5. reviewer pass + publish
- Future improvements:
  - replace placeholder `future_review` with policy-driven reviewer logic
  - add hard-fail classes for cliche endings and heavy hedge density
  - auto-generate patch-ready edit objects for MIR

## 4) Narrative Diagnostics Observatory Pipeline
- Status: `Proposed` (all components exist)
- Primary goal: produce a single diagnostics bundle across semantic repetition, entropy, burstiness, surprisals, and themes.
- Current building blocks:
  - `scripts/analyzer.py`
  - `scripts/signal_density.py`
  - `scripts/surprisal_scout.py`
  - `scripts/entropy_evaluator.py`
  - `scripts/burst_monitor.py`
  - `scripts/theme_mapper.py`
  - `scripts/pattern_extractor.py`
- Suggested stage flow:
  1. pre-processing
  2. run all diagnostics in parallel
  3. merge metrics into one report contract
  4. emit human report + machine-readable JSON summary
- Future improvements:
  - standardize shared metrics schema
  - add baseline comparison across manuscript versions
  - produce trend dashboards under `outputs/` and `mosaic_outputs/`

## 5) Lexical Upgrade Pipeline
- Status: `Proposed`
- Primary goal: increase lexical precision and reduce overuse clusters while preserving meaning.
- Current building blocks:
  - `scripts/word_frequency_benchmark.py`
  - `scripts/lexical_entropy_amplifier.py`
  - `scripts/lexical_enhancer.py`
  - `scripts/vivid_verb_upgrader.py`
- Suggested stage flow:
  1. detect overused terms
  2. generate rewrite options
  3. apply LM rewrite candidates
  4. interactive accept/reject loop
  5. validate post-edit entropy/signal deltas
- Future improvements:
  - add paragraph-level rollback and audit trail
  - gate substitutions with NER/entity-preservation checks
  - include language-specific lexical resources for translation outputs

## 6) Dialogue and Typography Precision Pipeline
- Status: `Proposed`
- Primary goal: enforce punctuation, quotation, and formatting consistency.
- Current building blocks:
  - `scripts/quotation_delimiter_auditor.py`
  - `scripts/typographic_precision_review.py`
  - `scripts/grammar_auditor.py`
- Suggested stage flow:
  1. run quote/delimiter audit
  2. run typography audit
  3. run grammar cross-check
  4. aggregate by paragraph and severity
- Future improvements:
  - add autofix mode for safe punctuation normalizations
  - enforce locale-aware typography profiles per target language

## 7) Mosaic Orchestrator Pipeline
- Status: `Active`
- Primary goal: parallel tool sweep with fidelity-context packaging and culling directives.
- Current building blocks:
  - `mosaic_mo.py`
  - `tool_wrapper.py`
  - `scripts/paragraph_issue_bundle.py`
  - `scripts/culling_resolver.py`
- Suggested stage flow:
  1. NLTK preflight
  2. parallel tool execution
  3. objective/proposal artifact generation
  4. issue bundling and culling resolution
  5. final directives report
- Future improvements:
  - migrate into Seedpipe spec/stages for compile-time validation
  - add stage-level retry policy and resumable state locks
  - add deterministic run manifest hashes for all artifacts

## 8) Human-in-the-Loop Refinement Pipeline
- Status: `Active` (manual loop)
- Primary goal: iterative acceptance of rewrite proposals with traceability.
- Current building blocks:
  - `scripts/mosaic_refiner.py`
  - `scripts/confidence_review.py`
  - `scripts/html_review.py`
  - `mosaic_work/session_state.json`
- Suggested stage flow:
  1. load manuscript + edits
  2. fuzzy-anchor paragraph targeting
  3. generate refined candidates
  4. approve/reject/regenerate loop
  5. save session and final manuscript
- Future improvements:
  - export accepted edits to patch format
  - support multi-reviewer adjudication and conflict resolution
  - add automatic post-acceptance quality recheck

## 9) Voice Rendering and Audio Publish Pipeline
- Status: `Active`
- Primary goal: generate audiobook-style MP3 output from markdown manuscript input.
- Current building blocks:
  - `pipes/voice-pipeline/spec/phase1/pipeline.yaml`
  - `pipes/voice-pipeline/src/stages/voice.py`
  - `scripts/kokoro_paragraph_reader.py`
- Suggested stage flow:
  1. manuscript ingest and markdown normalization
  2. Kokoro synthesis to WAV
  3. ffmpeg conversion to MP3
  4. publish artifact manifest
- Future improvements:
  - paragraph-level timestamps for audio navigation
  - multilingual voice profiles mapped to translation outputs
  - quality checks for clipping/silence and failed segments

## 10) Web Content Sync and Publish Pipeline
- Status: `Proposed`
- Primary goal: move approved manuscripts/translations into web content safely.
- Current building blocks:
  - `web/server.js`
  - `web/data/*`
  - `deploy/data/archive/*`
- Suggested stage flow:
  1. validate content shape and naming
  2. sync finalized markdown into `web/data/<lang>/`
  3. archive previous version
  4. run API smoke checks for language/chapter endpoints
- Future improvements:
  - atomic versioned publish directories
  - content integrity hashes and rollback pointer
  - CI checks that prevent broken chapter references

## 11) Event Sourcing Extraction Pipeline
- Status: `Proposed`
- Primary goal: transform manuscript events into graph-ready event sourcing records.
- Current building blocks:
  - `docs/event_sourcing_spec.md`
  - `schema.cypher`
  - `migrations/001_years.cypher`
  - `migrations/002_seed.cypher`
- Suggested stage flow:
  1. detect event candidates in manuscript
  2. map to `uid/type/timestamp/location/actors`
  3. validate against event schema rules
  4. emit Cypher import payloads
  5. apply migrations/updates
- Future improvements:
  - add causal-chain consistency checks
  - add idempotent re-import support per run
  - attach provenance links to source paragraph IDs

## 12) Contract and Schema Validation Pipeline
- Status: `Proposed`
- Primary goal: enforce artifact contract integrity across scripts and pipes.
- Current building blocks:
  - `scripts/schema_validator.py`
  - `schemas/*.schema.json`
  - `pipes/*/spec/phase1/contracts/*.schema.json`
- Suggested stage flow:
  1. collect produced JSON/JSONL artifacts
  2. validate records against expected schemas
  3. classify failures by stage and contract
  4. gate downstream publish/merge
- Future improvements:
  - make validation mandatory in generated wrappers
  - add schema version compatibility matrix
  - generate auto-fix hints for common shape mismatches

## 13) Voice Anchoring & Consistency Engine (VACE)
- Status: `Proposed`
- Primary goal: prevent voice drift across scenes, chapters, or co-written sections—critical for long series or multi-author projects.
- Description: Extracts character idiolect signatures from established arcs and compares new passages against them to detect inconsistencies in lexical entropy, syntactic fingerprints, and emotion-diction alignment.
- Suggested stage flow:
  1. character idiolect signature extraction (from character cards + prior text)
  2. lexical entropy computation (vocabulary richness per character)
  3. syntactic fingerprint analysis (avg clause length, subordination ratio)
  4. emotion-diction alignment scoring (e.g., detecting sarcastic markers in "I'm fine")
  5. deviation flagging with context (e.g., archaic contractions vs. modern phrasing)
  6. deliberate voice shift detection (trauma, disguise, intoxication via narrative intent cues)
  7. dialogue partner reciprocity checking (e.g., character softens speech addressing a child)
- Edge-case handling:
  - Detects deliberate voice shifts via narrative intent cues ("Her voice cracked—unlike her usual precision")
  - Compares against dialogue partner's voice for dynamic reciprocity
- Why it matters: Prevents "narrative amnesia" where characters sound like strangers in later chapters. Critical for series fidelity and emotional resonance.

## 14) Tension & Momentum Mapper (TaMM)
- Status: `Proposed`
- Primary goal: quantify and calibrate reader engagement at paragraph-level beyond simple word count metrics.
- Description: Trains a lightweight transformer on engagement signals (cliffhanger placement, question density, sensory urgency, pronoun shifts) to output tension gradients and momentum scores per paragraph.
- Suggested stage flow:
  1. engagement signal detection (cliffhangers, questions, sensory urgency, pronoun shifts)
  2. tension gradient calculation (0-1: how much does this paragraph raise stakes?)
  3. momentum score computation (0-1: verb density, clause chaining, time compression markers)
  4. surprise-clarity tradeoff measurement (entropic surprise vs. predictability)
  5. narrative pulse chart generation (spikes = tension peaks, valleys = reflective pauses)
  6. anti-pacing recognition (deliberate slowness for dread: "The knife hovered. Three seconds passed.")
  7. false tension flagging (frantic action with no stakes)
- Edge-case handling:
  - Recognizes anti-pacing and rewards controlled deceleration
  - Flags "false tension" scenarios
- Why it matters: Prevents "reader fatigue" or "boredom spikes." Helps authors orchestrate emotional arcs—not just tell stories.

## 15) Subtext Decoder & Emotional Undercurrent Scanner (S-DUCk)
- Status: `Proposed`
- Primary goal: audit subtextual coherence—ensuring what characters don't say aligns with their goals, secrets, and emotional blind spots.
- Description: Maps ostensible vs. latent topics in dialogue, checks for emotional dissonance between words and stage directions, and tracks revealed knowledge gaps.
- Suggested stage flow:
  1. ostensible vs. latent topic mapping (using contrastive LLM prompts: "What is the character avoiding saying?")
  2. emotional dissonance detection (words vs. body language: "I'm fine" + clenched fists)
  3. knowledge gap tracking (character knows X but pretends ignorance: over-explaining, misdirection via question tags)
  4. subtext map generation (color-coded layers: said vs. implied vs. dangerously unsaid)
  5. unreliable narrator flagging (contradictions within same passage: "I never lied" + memory flash contradicts)
  6. cultural context awareness (high-context vs. low-context communication norms)
- Edge-case handling:
  - Handles unreliable narrators by flagging internal contradictions
  - Respects cultural communication norms
- Why it matters: Great fiction lives in the unsaid. This tool prevents flat characters and ensures subtext earns its ambiguity.

## 16) Metaphor & Symbolic Resonance Tracker (MSRT)
- Status: `Proposed`
- Primary goal: audit the lifecycle of symbolic motifs across a novel—ensuring metaphors evolve, not repeat.
- Description: Builds a symbolic ontology tracking semantic drift, flags overuse/underuse, detects inconsistent logic, and suggests symbolic callbacks.
- Suggested stage flow:
  1. symbolic ontology construction (e.g., "water = memory, danger, rebirth" with evolving weights per chapter)
  2. semantic drift detection (water shifts from danger → memory → rebirth across chapters)
  3. overuse/underuse flagging (>3 water references in 2 pages = dilutes impact; key symbol vanishes mid-novel)
  4. inconsistent logic detection (fire = destruction in ch.2 but purity in ch.5 without thematic setup)
  5. symbolic callback suggestions (echo earlier metaphors: "A river in ch.5 could echo the drowning metaphor from ch.1")
  6. deliberate symbol collapse handling (motif "dies" to show thematic death: "The old gods' river had run dry.")
- Edge-case handling:
  - Handles deliberate symbol collapse for thematic effect
  - Works across languages with different mythic weights
- Why it matters: Symbols are the unseen architecture of meaning. This tool ensures they're not just decorative—but thinking tools for the reader.

## 17) Ethical Echo Chamber Detector (EECD)
- Status: `Proposed`
- Primary goal: prevent accidental reinforcement of harmful tropes—even when depicting them critically.
- Description: Scans for narrative alignment (critique vs. reproduction), maps power asymmetry in scenes, and flags empathy deserts.
- Suggested stage flow:
  1. narrative alignment scanning (Does the text critique a slur, or reproduce its emotional weight without commentary?)
  2. power asymmetry mapping (who interrupts whom, who gets the last line, whose perspective dominates)
  3. empathy desert flagging (marginalized character's trauma described but opaque internal life; oppressor gets rich interiority)
  4. responsible framing analysis (ensures authors write critically without self-censoring)
- Why it matters: Ethics isn't just about what you say—it's about how the narrative frames it. This tool helps authors write responsibly.

## 18) Epistemic Asymmetry Matrix (EAM)
- Status: `Proposed`
- Primary goal: map and optimize the "Knowledge Gap" (dramatic irony and suspense) across the narrative timeline.
- Description: Creates an ongoing "Truth Ledger" for four entities (POV Character, Opposing Character, Reader, "Archivist"/Objective Reality) and calculates epistemic deltas per scene.
- Suggested stage flow:
  1. truth ledger construction (POV Character, Opposing Character, Reader, Archivist knowledge states)
  2. epistemic delta calculation per scene (knowledge gap measurement)
     - Suspense mode: Reader knows 100%, POV knows 20% (reader knows bomb is under table; character doesn't)
     - Mystery mode: POV knows 100%, Reader knows 20% (founder executes master plan, reader kept in dark)
  3. epistemic stasis detection (alert if knowledge gap remains stagnant too long = boredom)
  4. plot hole/hallucination flagging (character acts on information they haven't acquired in text)
- Why it matters: Ensures the reader feels the exact friction of incomplete data. Critical for narratives with dramatic irony or mystery structures.

## 19) Somatic-to-Abstract Ratio Balancer (SARB)
- Status: `Proposed`
- Primary goal: manage the "Thermodynamic Cost" of prose—balancing philosophical abstraction with physical, somatic reality.
- Description: Tags sentences on a spectrum from hyper-somatic (physical) to hyper-abstract (ideological) to prevent reader disengagement.
- Suggested stage flow:
  1. sentence classification (Level 1: Hyper-Somatic "The blood pooled..." to Level 10: Hyper-Abstract "Equality was a thermodynamic error.")
  2. cognitive load calculation (if text spends 3 paragraphs at 8-10 range, reader experiences "Entropy")
  3. abstract bloat detection (flags when theoretical drift exceeds threshold)
  4. somatic anchor injection (advises: "Inject a Level 1 sensory detail here to pull reader back into the meat")
  5. grounding correction application
- Why it matters: Highly philosophical prose risks reading like a manifesto. SARB ensures every abstract critique is "paid for" with visceral physical grounding.

## 20) Fractal Resolution/Focal-Length Scanner (FRS)
- Status: `Proposed`
- Primary goal: audit the "Camera Work" of the narrative—ensuring seamless zooming between microscopic and cosmic scales.
- Description: Evaluates the "Scale" of each paragraph (Micro, Meso, Macro) and detects "Focal Lock" where scenes remain flat.
- Suggested stage flow:
  1. spatial & temporal scale tagging
     - Micro: dilated pupil, micro-second hesitation, single breath
     - Meso: conversation across a table, walk across a compound
     - Macro: collapse of a nation, sweep of a century, turning of the Earth
  2. focal lock detection (if scene remains at Meso level too long = "Flat")
  3. snap-zoom suggestion ("You describe geopolitical collapse (Macro). Pivot to Subject Zero's physical reaction (Micro) for vertigo effect.")
  4. depth of field optimization
- Why it matters: Master authors zoom seamlessly between scales. FRS ensures the prose possesses "terrifying, non-human depth of field."

## 21) Cognitive Dissonance & "The Shiver" Compiler (CDC)
- Status: `Proposed`
- Primary goal: automate detection and generation of the "Uncanny"—pairing brutal actions with beautiful aesthetics to create biological attraction to monstrous things.
- Description: Cross-references text to ensure Apex actions are paired with conflicting sensory data, generating the "Shiver" effect.
- Suggested stage flow:
  1. orthogonal pairing engine (scans for standard expected emotional pairings: "cruel and ugly" = "Aggregate Slop")
  2. friction generation (if action is brutal: check if aesthetic is beautiful/calm/sacred; if action is intimate: check for predation markers)
  3. shiver index calculation (score based on bypassing prefrontal cortex to strike autonomic nervous system)
  4. soft-word enforcement (ensures soft actions don't use soft words)
- Why it matters: This is the "Honey Trap"—mathematically enforcing the stylistic rule: Do not use soft words for soft actions. Actively short-circuits reader conditioning line by line.

---

## Writing Enhancement Pipeline Bundle (13-21)

These nine pipelines (13-21) form a cohesive Writing Enhancement Suite focused on quality, consistency, and reader engagement:

| Pipeline | Primary Focus | Key Metric |
|----------|---------------|------------|
| VACE | Character voice consistency | Idiolect deviation score |
| TaMM | Reader engagement calibration | Tension gradient, momentum score |
| S-DUCk | Subtext coherence | Subtext map completeness |
| MSRT | Symbolic motif evolution | Semantic drift index |
| EECD | Ethical narrative framing | Power asymmetry, empathy balance |
| EAM | Knowledge gap optimization | Epistemic delta per scene |
| SARB | Abstract/somatic balance | Cognitive load, grounding frequency |
| FRS | Narrative scale variation | Focal lock detection |
| CDC | Uncanny dissonance generation | Shiver index |

These pipelines share common infrastructure needs:
- LLM-based analysis stages with structured output schemas
- Threshold-based flagging for human review
- Visualization components (narrative pulse charts, subtext maps, focal length overlays)
- Integration with character cards and manuscript metadata

---

---

## Cross-Pipeline Improvements (Recommended)
1. Define a single canonical artifact contract for manuscript units (`paragraph_id`, `content_hash`, stable ordering) and reuse it in all pipelines.
2. Standardize run metadata (`run_id`, attempt, manifest hash, timestamps, model metadata) across scripts and Seedpipe flows.
3. Add a shared quality scorecard contract so diagnostics and reviewers can gate on common thresholds.
4. Create a pipeline test harness with fixtures in `docs/` or `artifacts/inputs/` and expected outputs for regression checks.
5. Promote ad hoc script chains into formal Seedpipe specs where deterministic stage I/O and retries matter.
6. Add CI jobs for schema validation, smoke runs, and web content integrity after translation/refinement changes.
7. Build a small run registry index (JSON or SQLite) for comparing quality trends across runs and manuscript versions.

## Suggested Implementation Order
1. Translation Quality Gate Pipeline
2. Narrative Diagnostics Observatory Pipeline
3. Slop Stop advanced stages (`future_review` replacement + policy gating)
4. Contract and Schema Validation Pipeline
5. Web Content Sync and Publish Pipeline
6. Event Sourcing Extraction Pipeline
7. Voice Anchoring & Consistency Engine (VACE) - foundational for character-driven works
8. Tension & Momentum Mapper (TaMM) - quick wins for reader engagement
9. Epistemic Asymmetry Matrix (EAM) - critical for mystery/suspense structures
10. Somatic-to-Abstract Ratio Balancer (SARB) - prevents philosophical bloat
11. Subtext Decoder & Emotional Undercurrent Scanner (S-DUCk) - deepens character work
12. Fractal Resolution/Focal-Length Scanner (FRS) - elevates prose craft
13. Metaphor & Symbolic Resonance Tracker (MSRT) - thematic coherence
14. Cognitive Dissonance & "The Shiver" Compiler (CDC) - stylistic signature enforcement
15. Ethical Echo Chamber Detector (EECD) - responsible storytelling
