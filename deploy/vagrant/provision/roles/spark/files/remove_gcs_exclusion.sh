#!/usr/bin/env bash

python << EOF
import re

PATTERN = r'''<exclusion>
\s*<groupId>org\.alluxio\.alluxio</groupId>
\s*<artifactId>alluxio-underfs-gcs</artifactId>
\s*</exclusion>'''
POM = '/spark/core/pom.xml'

new_pom = re.compile(PATTERN).sub('', open(POM).read())
open(POM, 'w').write(new_pom)
EOF
