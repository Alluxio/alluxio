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

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

public class PersistFileEntry extends JournalEntry {
  private final long mFileId;
  private final long mLength;
  private final long mOpTimeMs;

  @JsonCreator
  public PersistFileEntry(@JsonProperty("fileId") long fileId, @JsonProperty("length") long length,
      @JsonProperty("operationTimeMs") long opTimeMs) {
    mFileId = fileId;
    mLength = length;
    mOpTimeMs = opTimeMs;
  }

  @JsonGetter
  public long getFileId() {
    return mFileId;
  }

  @JsonGetter
  public long getLength() {
    return mLength;
  }

  @JsonGetter
  public long getOperationTimeMs() {
    return mOpTimeMs;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.ADD_CHECKPOINT;
  }
}
