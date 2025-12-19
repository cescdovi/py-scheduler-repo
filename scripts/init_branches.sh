#!/usr/bin/env bash
set -euo pipefail
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_DIR"

if [ ! -d .git ]; then
  git init
  git add -A
  git commit -m "Initial: common Python files"
fi

# main: polling
git checkout -B main
cp -f variants/polling/scheduler.py ./scheduler.py
git add scheduler.py
git commit -m "main: polling scheduler (Python)"

# student: watch skeleton
git checkout -B skeleton
cp -f variants/watch-skeleton/scheduler.py ./scheduler.py
git add scheduler.py
git commit -m "student: watch-based skeleton (Python)"

# solution: watch solution
git checkout -B solution
cp -f variants/watch-solution/scheduler.py ./scheduler.py
git add scheduler.py
git commit -m "solution: watch-based scheduler (Python)"

git checkout main
echo "Branches created: main (polling), student (watch skeleton), solution (watch solution)"
