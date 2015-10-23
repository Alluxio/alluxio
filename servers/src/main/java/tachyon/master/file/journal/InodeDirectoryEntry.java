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
import com.fasterxml.jackson.annotation.JsonProperty;

import tachyon.master.file.meta.InodeDirectory;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represents a jounal entry for a directory inode.
 */
public class InodeDirectoryEntry extends InodeEntry {

  /**
   * Creates a new instance of {@link InodeDirectory}.
   *
   * @param creationTimeMs the creation time (in milliseconds)
   * @param id the id
   * @param name the name
   * @param parentId the parent id
   * @param persisted the persisted flag
   * @param pinned the pinned flag
   * @param lastModificationTimeMs the last modification time (in milliseconds)
   */
  @JsonCreator
  public InodeDirectoryEntry(
      @JsonProperty("creationTimeMs") long creationTimeMs,
      @JsonProperty("id") long id,
      @JsonProperty("name") String name,
      @JsonProperty("parentId") long parentId,
      @JsonProperty("persisted") boolean persisted,
      @JsonProperty("pinned") boolean pinned,
      @JsonProperty("lastModificationTimeMs") long lastModificationTimeMs) {
    super(creationTimeMs, id, name, parentId, persisted, pinned, lastModificationTimeMs);
  }

  /**
   * Converts the entry to {@link InodeDirectory}.
   *
   * @return the {@link InodeDirectory} representation
   */
  public InodeDirectory toInodeDirectory() {
    InodeDirectory inode =
        new InodeDirectory.Builder()
            .setName(mName)
            .setId(mId)
            .setParentId(mParentId)
            .setCreationTimeMs(mCreationTimeMs)
            .setPersisted(mPersisted)
            .build();
    inode.setPersisted(mPersisted);
    inode.setPinned(mPinned);
    inode.setLastModificationTimeMs(mLastModificationTimeMs);
    return inode;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.INODE_DIRECTORY;
  }
}
