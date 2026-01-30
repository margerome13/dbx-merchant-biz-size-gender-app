#!/bin/bash
# Script to push the updated csv_upload_to_table.py to GitHub

echo "Pushing CSV upload fix to GitHub..."

cd "$(dirname "$0")"

git add views/csv_upload_to_table.py
git commit -m "fix: Improve user email detection in CSV upload - add multiple fallback methods"
git push origin main

echo "Done! Check if push was successful above."
