#!/bin/sh
msg=$(grep "^$GIT_COMMIT " .git/commit-map.txt | sed 's/^[^ ]* //')
if [ -n "$msg" ]; then
  printf '%s\n' "$msg"
else
  cat
fi
