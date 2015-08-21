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

import tachyon.master.next.journal.JournalEntry;
import tachyon.master.next.journal.JournalEntryType;

public abstract class InodeEntry implements JournalEntry {
  private final long mCreationTimeMs;
  private final long mId;
  private final String mName;
  private final long mParentId;
  private final boolean mIsPinned;
  private final long mLastModificationTimeMs;

  public InodeEntry(long creationTimeMs, long id, String name, long parentId, boolean isPinned,
      long lastModificationTimeMs) {
    mCreationTimeMs = creationTimeMs;
    mId = id;
    mName = name;
    mParentId = parentId;
    mIsPinned = isPinned;
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  public long creationTimeMs() {
    return mCreationTimeMs;
  }

  public long id() {
    return mId;
  }

  public String name() {
    return mName;
  }

  public long parentId() {
    return mParentId;
  }

  public boolean isPinned() {
    return mIsPinned;
  }

  public long lastModificationTimeMs() {
    return mLastModificationTimeMs;
  }

  @Override
  public abstract JournalEntryType type();
}
