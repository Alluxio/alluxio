/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

#ifndef CLIJNIFUSE_OPTIONPARSER_H
#define CLIJNIFUSE_OPTIONPARSER_H

#include <map>
#include <cstdio>
#include <cstdlib>
#include "command/include/metadatacache.h"
#include "command.h"

using namespace std;

class OptionParser {
public:
  OptionParser(int &argc, char **&argv)
    : argc(argc), argv(argv) {
  };
  void usage();
  Command* getCommand();
private:
  int &argc;
  char **&argv;
};
#endif //CLIJNIFUSE_OPTIONPARSER_H
