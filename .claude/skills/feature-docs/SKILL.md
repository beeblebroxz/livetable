---
name: feature-docs
description: Use when a livetable feature or API change is finished and about to be committed — lists every doc to update (README, Python bindings reference, vision status, wire protocol, onboarding, pipeline and benchmark docs, invariant rules) and how to treat docs/superpowers plans.
---

# Docs to update when a feature ships

Update every doc the change touches before committing:

1. **README.md** - Features list and Example section
2. **docs/PYTHON_BINDINGS_README.md** - Full API reference and examples: constructor, methods, parameters
3. **docs/ORIGINAL_VISION.md** - Mark implemented features as complete in Implementation Status; update Future Enhancements if applicable
4. **docs/WEBSOCKET_PROTOCOL.md** - Wire changes, sequencing, and limits (server feature only)
5. **QUICK_START.md**, **docs/GETTING_STARTED.md**, **docs/START_HERE.md** - Onboarding and runnable examples
6. **docs/API_GUIDE.md**, **docs/JOIN_FEATURE.md**, **tests/README.md** - Rust/API contracts and actual local/CI checks
7. **docs/INCREMENTAL_*_PIPELINE.md**, **docs/PERFORMANCE_COMPARISON.md** - Propagation limits and reproducible benchmarks; preserve measured commit/environment provenance
8. **.claude/rules/*.md** - The subsystem rule whose invariant changed

New Python API also needs tests in `tests/python/`.

Plans/specs under `docs/superpowers/` are historical records, not current task instructions. Keep their implementation status and current-reference links accurate without rewriting original checklists as if they were executed today.
