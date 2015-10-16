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

package tachyon.master.file.journal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

public final class ReinitializeFileEntry extends JournalEntry {
  private final String mPath;
  private final long mBlockSizeBytes;
  private final long mTTL;

  @JsonCreator
  public ReinitializeFileEntry(@JsonProperty("path") String path,
      @JsonProperty("blockSizeBytes") long blockSizeBytes, @JsonProperty("ttl") long ttl) {
    mPath = Preconditions.checkNotNull(path);
    mBlockSizeBytes = blockSizeBytes;
    mTTL = ttl;
  }

  @JsonGetter
  public String getPath() {
    return mPath;
  }

  @JsonGetter
  public long getBlockSizeBytes() {
    return mBlockSizeBytes;
  }

  @JsonGetter("ttl")
  public long getTTL() {
    return mTTL;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.REINITIALIZE_FILE;
  }

}
