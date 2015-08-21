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

import tachyon.TachyonURI;
import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalEntryType;

public class AddCheckpointEntry implements JournalEntry {
  private final int mFileId;
  private final long mLength;
  private final TachyonURI mCheckpointPath;
  private final long mOpTimeMs;

  public AddCheckpointEntry(int fileId, long length, TachyonURI checkpointPath, long opTimeMs) {
    mFileId = fileId;
    mLength = length;
    mCheckpointPath = checkpointPath;
    mOpTimeMs = opTimeMs;
  }

  public int fileId() {
    return mFileId;
  }

  public long length() {
    return mLength;
  }

  public TachyonURI checkpointPath() {
    return mCheckpointPath;
  }

  public long opTimeMs() {
    return mOpTimeMs;
  }

  @Override
  public JournalEntryType type() {
    return JournalEntryType.ADD_CHECKPOINT;
  }
}
