#!/usr/local/bin/python3

import os
import sys
from functools import cmp_to_key
from random import shuffle

def import_filter(line: str):
  return line.startswith("import ")

def static_filter(line: str):
  return line.startswith("import static ")

def special_filter(line: str):
  return line.startswith("import alluxio")

def standard_java_filter(line: str):
  return line.startswith("import java") or line.startswith("import javax")

def third_party_filter(line: str):
  return not (
    static_filter(line) or
    special_filter(line) or
    standard_java_filter(line)
  )

def find_first(l, pred):
  for i, v in enumerate(l):
    if pred(v):
      return i

def main(argv):
  rootdir = "." if len(argv) == 1 else argv[1]

  for root, subdir, files in os.walk(rootdir):
    for file in files:
      if not file.endswith(".java"):
        continue
      
      print(f"\nroot:{root}, subdir:{subdir}, file:{file}")

      with open(os.path.join(root, file), 'r+') as f:
        lines = f.readlines()
        imports = set(filter(import_filter, lines))

        imports_sorted = []
        for filtre in [static_filter, special_filter, third_party_filter, standard_java_filter]:
          items: list = list(filter(filtre, imports))
          if not items:
            continue
          items.sort()
          imports_sorted += items[:] + ["\n"]

        start = find_first(lines, import_filter)
        try:
          end = len(lines) - find_first(reversed(lines), import_filter)
        except:
          continue
        newlines = lines[:start] + imports_sorted[:-1] + lines[end:]

      with open(os.path.join(root, file), 'w') as f:
        f.writelines(newlines)


if __name__ == '__main__':
  main(sys.argv)
