/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.next.filesystem.journal;

import java.util.Map;

import com.google.common.collect.Maps;

import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalEntryType;

public class CreateFileEntry implements JournalEntry {
  private final String mPath;
  private final long mBlockSizeBytes;
  private final boolean mRecursive;
  private final long mCreationTimeMs;

  public CreateFileEntry(String path, long blockSizeBytes, boolean recursive, long creationTimeMs) {
    mPath = path;
    mBlockSizeBytes = blockSizeBytes;
    mRecursive = recursive;
    mCreationTimeMs = creationTimeMs;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.CREATE_FILE;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(4);
    parameters.put("path", mPath);
    parameters.put("blockSizeBytes", mBlockSizeBytes);
    parameters.put("recursive", mRecursive);
    parameters.put("creationTimeMs", mCreationTimeMs);
    return parameters;
  }
}
