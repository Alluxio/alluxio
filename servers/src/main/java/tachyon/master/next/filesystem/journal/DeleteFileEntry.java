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

public class DeleteFileEntry implements JournalEntry {
  public final long mFileId;
  public final boolean mRecursive;
  public final long mOpTimeMs;

  public DeleteFileEntry(long fileId, boolean recursive, long opTimeMs) {
    mFileId = fileId;
    mRecursive = recursive;
    mOpTimeMs = opTimeMs;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.DELETE_FILE;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(3);
    parameters.put("fileId", mFileId);
    parameters.put("recursive", mRecursive);
    parameters.put("operationTimeMs", mOpTimeMs);
    return parameters;
  }
}
