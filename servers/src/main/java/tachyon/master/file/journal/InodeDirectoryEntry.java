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

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import tachyon.master.file.meta.InodeDirectory;
import tachyon.master.journal.JournalEntryType;

public class InodeDirectoryEntry extends InodeEntry {
  private Set<Long> mChildrenIds;

  @JsonCreator
  public InodeDirectoryEntry(@JsonProperty("creationTimeMs") long creationTimeMs,
      @JsonProperty("id") long id, @JsonProperty("name") String name,
      @JsonProperty("parentId") long parentId, @JsonProperty("persisted") boolean persisted,
      @JsonProperty("pinned") boolean pinned,
      @JsonProperty("lastModificationTimeMs") long lastModificationTimeMs,
      @JsonProperty("childrenIds") Set<Long> childrenIds) {
    super(creationTimeMs, id, name, parentId, persisted, pinned, lastModificationTimeMs);

    mChildrenIds = childrenIds;
  }

  public InodeDirectory toInodeDirectory() {
    InodeDirectory inode =
        new InodeDirectory.Builder().setName(mName).setId(mId).setParentId(mParentId)
            .setCreationTimeMs(mCreationTimeMs).setPersisted(mPersisted).build();
    inode.setPersisted(mPersisted);
    inode.setPinned(mPinned);
    inode.setLastModificationTimeMs(mLastModificationTimeMs);
    return inode;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.INODE_DIRECTORY;
  }

  @JsonGetter("childrenIds")
  public Set<Long> getChildrenIds() {
    return mChildrenIds;
  }
}
