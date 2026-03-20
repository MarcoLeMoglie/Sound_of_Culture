#!/bin/bash

# Backup Script - Sound of Culture

echo "Updating Folder Structure in README.md..."
python visualize_structure.py

echo "Checking Git Status..."
if [ ! -d ".git" ]; then
    echo "Initializing Git Repository..."
    git init
    # Set default branch to main
    git branch -M main
fi

# Add all changes
git add .

# Prompt for commit message or use default
commit_msg=$1
if [ -z "$commit_msg" ]; then
    commit_msg="Update structure and progress: $(date +'%Y-%m-%d %H:%M:%S')"
fi

echo "Committing with message: $commit_msg"
git commit -m "$commit_msg"

# Check if remote origin is set
if git remote | grep -q "origin"; then
    echo "Pushing to GitHub..."
    git push -u origin main
else
    echo "WARNING: 'origin' remote not set. Run 'git remote add origin <URL>' to enable GitHub backup."
fi

echo "Backup complete!"
