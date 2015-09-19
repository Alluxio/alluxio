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

import java.util.Map;

import com.google.common.base.Objects;
import com.google.common.collect.Maps;

import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

public abstract class InodeEntry implements JournalEntry {
  protected final long mId;
  protected final long mParentId;
  protected final String mName;
  protected final boolean mIsPinned;
  protected final long mCreationTimeMs;
  protected final long mLastModificationTimeMs;

  public InodeEntry(long creationTimeMs, long id, String name, long parentId, boolean isPinned,
      long lastModificationTimeMs) {
    mId = id;
    mParentId = parentId;
    mName = name;
    mIsPinned = isPinned;
    mCreationTimeMs = creationTimeMs;
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  @Override
  public abstract JournalEntryType getType();

  @Override
  public Map<String, Object> getParameters() {
    Map<String, Object> parameters = Maps.newHashMapWithExpectedSize(6);
    parameters.put("id", mId);
    parameters.put("parentId", mParentId);
    parameters.put("name", mName);
    parameters.put("isPinned", mIsPinned);
    parameters.put("creationTimeMs", mCreationTimeMs);
    parameters.put("lastModificationTimeMs", mLastModificationTimeMs);
    return parameters;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mId, mParentId, mName, mIsPinned, mCreationTimeMs,
        mLastModificationTimeMs);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof InodeEntry) {
      InodeEntry that = (InodeEntry) object;
      return Objects.equal(this.mId, that.mId) && Objects.equal(this.mParentId, that.mParentId)
          && Objects.equal(this.mName, that.mName) && Objects.equal(this.mIsPinned, that.mIsPinned)
          && Objects.equal(this.mCreationTimeMs, that.mCreationTimeMs)
          && Objects.equal(this.mLastModificationTimeMs, that.mLastModificationTimeMs);
    }
    return false;
  }
}
