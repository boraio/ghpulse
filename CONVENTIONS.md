# Conventions

## Tests
- Everything in tests/ runs offline. No network, no object storage, no credentials.
  Synthetic fixtures only.
- tests/test_transform.py is a standalone script run as `python tests/test_transform.py`,
  not pytest. Add checks inside `main()` using the existing `check()` helper.
- Never remove or restructure an existing check. Adding is fine.
- CI validates the code, not the data.

## Comments
- Explain WHY, not what the next line does.
- Never state a claim about a tool's behaviour unless it has been verified.
  If unsure, say so in the comment rather than asserting.

## Environment
- Virtualenv is .ghpulsevenv. Dependencies go in requirements.txt.
- Secrets live in /etc/jenga.env, loaded via shell helpers. Never in the repo.

## Style
- Bronze is immutable per-partition. Reprocessing is a re-run, not a mutation.
