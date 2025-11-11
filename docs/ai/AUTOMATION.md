# AUTOMATION

## Pre-commit reminder
Add a pre-commit hook to remind updating `.env.example` when envs change.

```bash
#!/usr/bin/env bash
set -euo pipefail
if ! git diff --cached --name-only | grep -q ".env.example"; then
  echo "[warn] Remember to update .env.example if envs changed." >&2
fi
```
Save as `.git/hooks/pre-commit` and make executable.

## CI: AI Context Snapshot
We keep a context snapshot for AIs at `docs/ai/CONTEXT.md` on every push to main.
