#!/bin/bash

if [ $(git diff --name-only --cached | grep -c '.tsx') != 0 ]; then
  lerna run lint-staged-tsx-files --parallel --stream
else
  echo 'No .tsx files to lint. Continuing with commit.'
fi
