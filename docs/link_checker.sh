#! /usr/bin/env bash

docs_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
cd $docs_dir
htmlproofer --allow-hash-href --file-ignore "/cn/,/pt/" --url-ignore "/localhost:*/" ./_site > htmlproofer.report 2>&1
