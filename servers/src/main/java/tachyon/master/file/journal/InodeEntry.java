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

/**
 * This abstract class represents a journal entry for an inode.
 */
public abstract class InodeEntry extends JournalEntry {
  protected final long mId;
  protected final long mParentId;
  protected final String mName;
  protected final boolean mPersisted;
  protected final boolean mPinned;
  protected final long mCreationTimeMs;
  protected final long mLastModificationTimeMs;

  /**
   * Creates a new instance of {@link InodeEntry}.
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
  public InodeEntry(
      @JsonProperty("creationTimeMs") long creationTimeMs,
      @JsonProperty("id") long id,
      @JsonProperty("name") String name,
      @JsonProperty("parentId") long parentId,
      @JsonProperty("persisted") boolean persisted,
      @JsonProperty("pinned") boolean pinned,
      @JsonProperty("lastModificationTimeMs") long lastModificationTimeMs) {
    mId = id;
    mParentId = parentId;
    mName = name;
    mPersisted = persisted;
    mPinned = pinned;
    mCreationTimeMs = creationTimeMs;
    mLastModificationTimeMs = lastModificationTimeMs;
  }

  /**
   * @return the creation time (in milliseconds)
   */
  @JsonGetter
  public long getCreationTimeMs() {
    return mCreationTimeMs;
  }

  /**
   * @return the id
   */
  @JsonGetter
  public long getId() {
    return mId;
  }

  /**
   * @return the parent id
   */
  @JsonGetter
  public long getParentId() {
    return mParentId;
  }

  /**
   * @return the name
   */
  @JsonGetter
  public String getName() {
    return mName;
  }

  /**
   * @return the persisted flag
   */
  @JsonGetter
  public boolean getPersisted() {
    return mPersisted;
  }

  /**
   * @return the pinned flag
   */
  @JsonGetter
  public boolean getPinned() {
    return mPinned;
  }

  /**
   * @return the last modification time (in milliseconds)
   */
  @JsonGetter
  public long getLastModificationTimeMs() {
    return mLastModificationTimeMs;
  }
}

