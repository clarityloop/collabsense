# CollabSense Paper — Agent Progress & TODOs

## Status Summary (2026-05-17)

### ✅ Completed
- **Growth Opportunities manual assessment** — Section 6.2 in paper
  - 20 items rated by 2 raters (Sachin + Ethan); GRO-020 had form bug, ratings provided manually
  - Inter-rater agreement: Cohen's κ = 0.76 (substantial); per-criterion: Actionable 0.68, Genuine 0.68, Supported 0.90
  - Consensus results: 85% actionable, 85% genuine, 90% evidence-supported (≥Partial)
  - All 7 disagreements on Yes/Partial boundary — no Yes/No splits
  - 3 failure cases documented with reviewer quotes (GRO-004, GRO-015, GRO-017)
  - Construct Validity section updated to reference assessment
- **ESEM tone research** — Tone guide, structure guide, phrase book created as skill (`esem-tone-editor`)
- **ESEM tone skill** — Created `/Users/jos/.copilot/skills/esem-tone-editor/` with full reference materials
- **Paper converted to LIPIcs format**

### 🔲 Remaining TODOs

#### High Priority
1. **Automated sentiment verification** — Use automated tools to verify the 3,300+ feedback scores since manual review isn't practical. Options:
   - Compare a sample against VADER/TextBlob baseline and report correlation
   - The verbosity bias check (Section 7) already partially addresses this
   - Could add a "convergent validity" argument: engine scores correlate with an independent sentiment tool

2. **Compile & page count check** — Verify paper renders correctly with LIPIcs and stays within 17-page SEIP limit

#### Medium Priority
3. **Growth opportunity methodology** — Sachin wants to discuss how growth opps are calculated. ClarityLoop devs may not want to disclose exact prompts. Options:
   - Add high-level description without disclosing proprietary prompts
   - Note proprietary nature as a replication threat (already partially in External Validity)
   - Download ClarityLoop repo and review prompting logic for a general description

4. **ESEM tone pass** — Run the `esem-tone-editor` skill on the full paper to match ESEM SEIP register (hedging, voice, structure). Do section-by-section to avoid context saturation.

### Notes
- Paper: `reports/collabsense_paper_latex.tex`
- Validation CSVs: `dataset/validation_*.csv`
- Response data: `~/Downloads/CollabSense Validation_ Growth Opportunities (Responses) - Form Responses 1 (1).csv`
- Deadline: Abstract May 20, Full paper May 27, 2026
- Submit via: esem26-seip.hotcrp.com
