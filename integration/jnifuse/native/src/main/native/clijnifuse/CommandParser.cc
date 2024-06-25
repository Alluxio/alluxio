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

#include "CommandParser.h"

enum COMMANDS_EUNM {
  METADATA_CACHE,
  FILE_INFO
};

std::map<std::string, COMMANDS_EUNM> COMMANDS_MAP =
  {{"metadatacache", METADATA_CACHE}};

void CommandParser::usage() {
  std::cout
    << "A tool to get information form fuse sdk. This program only supports linux OS now."
    << endl
    << endl
    << "Usage:" << "fusecli [COMMAND] [COMMAND_ARGS]"
    << endl
    << endl
    << "Support Commands:"
    << endl
    << "\tmetadatacache      Clear fuse client side metadata cache or get metadata cache size."
    << endl
    << "See sub-commands' descriptions for more details."
    << endl;
}

Command* CommandParser::getCommand() {
  Command *command;
  argc--;
  argv++;
  if (argc <= 0) {
    usage();
    exit(0);
  }

  map<string, COMMANDS_EUNM>::const_iterator i = COMMANDS_MAP.find(argv[0]);
  if (i == COMMANDS_MAP.end()) {
    usage();
    exit(0);
  }

  switch (i->second) {
    case METADATA_CACHE:
      command = new MetaDataCacheCommand(argc, argv);
      break;
    default:
      usage();
      break;
  }

  while (command->hasSubCommands()) {
    argc--;
    argv++;
    if (argc <= 0) {
      cout << command->getUsage() << endl;
      exit(0);
    }
    std::string subCmdName(argv[0]);
    command = command->getSubCommand(subCmdName);
  }

  return command;
}
