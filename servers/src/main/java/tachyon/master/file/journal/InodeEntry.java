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

import com.fasterxml.jackson.annotation.JsonGetter;

import tachyon.master.journal.JournalEntry;

public abstract class InodeEntry extends JournalEntry {
  protected final long mId;
  protected final long mParentId;
  protected final String mName;
  protected final boolean mPersisted;
  protected final boolean mPinned;
  protected final long mCreationTimeMs;
  protected final long mLastModificationTimeMs;

  public InodeEntry(long creationTimeMs, long id, String name, long parentId, boolean persisted,
      boolean pinned, long lastModificationTimeMs) {
    mId = id;
    mParentId = parentId;
    mName = name;
    mPersisted = persisted;
    mPinned = pinned;
    mCreationTimeMs = creationTimeMs;
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  @JsonGetter
  public long getmId() {
    return mId;
  }

  @JsonGetter
  public long getmParentId() {
    return mParentId;
  }

  @JsonGetter
  public String getName() {
    return mName;
  }

  @JsonGetter
  public boolean isPersisted() {
    return mPersisted;
  }

  @JsonGetter
  public boolean isPinned() {
    return mPinned;
  }

  @JsonGetter
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  @JsonGetter
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }
}
