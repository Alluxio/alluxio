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

#ifndef FUSE_NATIVE_CLI_COMMAND_H_
#define FUSE_NATIVE_CLI_COMMAND_H_

#include <iostream>
#include <string>

class Command {
 public:
  Command(int &argc, char **&argv) : argc(argc), argv(argv) {
  }
  virtual ~Command(){}
  virtual std::string getUsage() const = 0;
  virtual std::string getCommandName () const = 0;
  virtual bool hasSubCommands () const = 0;
  virtual Command* getSubCommand(std::string subCommandName) = 0;
  virtual bool hasOptions() const = 0;
  // Parse options, the return value should expend into struct.
  virtual void parseOptions() = 0;
  virtual void run() = 0;
protected:
  int &argc;
  char **&argv;
  char *mountPoint;
  char *path;
};
#endif // FUSE_NATIVE_CLI_COMMAND_H_