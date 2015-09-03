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

package tachyon.master.filesystem.journal;

import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

public final class CompleteFileEntry implements JournalEntry {
  private final List<Long> mBlockIds;
  private final long mId;
  private final long mLength;
  private final long mOpTimeMs;

  public CompleteFileEntry(List<Long> blockIds, long id, long length, long opTimeMs) {
    mBlockIds = Preconditions.checkNotNull(blockIds);
    mId = id;
    mLength = length;
    mOpTimeMs = opTimeMs;
  }

  public List<Long> getBlockIds() {
    return mBlockIds;
  }

  public long getFileId() {
    return mId;
  }

  public long getFileLength() {
    return mLength;
  }

  public long getOperationTimeMs() {
    return mOpTimeMs;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.COMPLETE_FILE;
  }

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(3);
    parameters.put("blockIds", mBlockIds);
    parameters.put("id", mId);
    parameters.put("length", mLength);
    parameters.put("operationTimeMs", mOpTimeMs);
    return parameters;
  }
}
