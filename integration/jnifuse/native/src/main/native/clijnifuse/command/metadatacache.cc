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

#include "include/clear.h"
#include "include/size.h"
#include "include/metadatacache.h"

enum METADATA_CACHE_COMMANDS_ENUM {
  CLEAR,
  SIZE
};

std::map<std::string, METADATA_CACHE_COMMANDS_ENUM> METADATA_CACHE_COMMANDS_MAP =
  {{"clear", CLEAR},{"size", SIZE}};

std::string MetaDataCacheCommand::getUsage() const {
  std::string usage = "Usage: fusecli metadatacache [clear|size] <command args>";
  usage.append("\nProvide operations for the fuse metadata. See sub-commands' descriptions for more details.");
  return usage;
}

Command* MetaDataCacheCommand::getSubCommand(std::string subCommandName) {
  std::map<std::string, METADATA_CACHE_COMMANDS_ENUM>::const_iterator i =
    METADATA_CACHE_COMMANDS_MAP.find(subCommandName);
  if (i == METADATA_CACHE_COMMANDS_MAP.end()) {
    cout << getUsage() << endl;
    exit(0);
  }
  argc--;
  argv++;
  Command* command = NULL;
  switch (i->second) {
    case CLEAR:
      command = new ClearMetaDataCache(argc, argv);
      break;
    case SIZE:
      command = new SizeMetaDataCache(argc, argv);
      break;
    default:
      cout << getUsage() << endl;
      exit(0);
  }
  return command;
}
