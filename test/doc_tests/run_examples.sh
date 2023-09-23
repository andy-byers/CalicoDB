#!/usr/bin/env bash

echo "run_examples.sh..."

for EXAMPLE in $1; do
  MESSAGE="running $EXAMPLE...\t"

  "$EXAMPLE"

  if [$? -eq 0]; then
    printf "$MESSAGE[PASS]\n"
  else
    printf "$MESSAGE[FAIL]\n"
  fi
done
