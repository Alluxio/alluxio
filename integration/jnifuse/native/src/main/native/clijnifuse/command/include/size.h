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

#ifndef CLIJNIFUSE_SIZE_H
#define CLIJNIFUSE_SIZE_H

#include "../../command.h"

using namespace std;

class SizeMetaDataCache : public Command {
public:
  SizeMetaDataCache(int &argc, char **&argv) : Command(argc, argv) {}
  ~SizeMetaDataCache(){}
  inline std::string getCommandName() const override {
    return "size";
  }
  std::string getUsage() const override;

  inline bool hasSubCommands() const override {
    return false;
  }
  inline bool hasOptions() const override {
    return true;
  }
  inline Command* getSubCommand(std::string subCommandName) override {
    return NULL;
  }
  void parseOptions() override;

  void run() override;
};

#endif //CLIJNIFUSE_SIZE_H
