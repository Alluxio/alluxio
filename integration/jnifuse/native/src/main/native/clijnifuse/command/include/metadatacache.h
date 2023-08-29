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

#ifndef CLIJNIFUSE_METADATACACHE_H
#define CLIJNIFUSE_METADATACACHE_H
#include <iostream>
#include <map>
#include <string>
#include <unistd.h>
#include "../../command.h"

using namespace std;

class MetaDataCacheCommand : public Command {
public:
  MetaDataCacheCommand(int &argc, char **&argv) : Command(argc, argv) {
  }
  ~MetaDataCacheCommand(){}

  std::string getUsage() const override;
  inline std::string getCommandName() const override {
    return "metadatacache";
  }
  inline bool hasSubCommands() const override {
    return true;
  }
  Command* getSubCommand(std::string subCommandName) override;
  void parseArgs() override {
    getUsage();
  }
  void run() override{
    getUsage();
  }
};
#endif //CLIJNIFUSE_METADATACACHE_H
