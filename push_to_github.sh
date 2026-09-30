#!/usr/bin/env bash
# Usage: ./push_to_github.sh https://github.com/<user>/Day19-Spark-Broadcast-Join.git
set -euo pipefail
REMOTE_URL="${1:?Provide the GitHub repository URL (create an empty repo on GitHub first)}"

[ -d .git ] || git init
git add .
git commit -m "Day19: Spark Broadcast Join vs Sort-Merge Join (PySpark)" || echo "Nothing new to commit"
git branch -M main
git remote remove origin 2>/dev/null || true
git remote add origin "$REMOTE_URL"
git push -u origin main
