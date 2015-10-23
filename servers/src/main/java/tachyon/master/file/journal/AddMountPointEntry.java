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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import tachyon.TachyonURI;
import tachyon.master.journal.JournalEntry;
import tachyon.master.journal.JournalEntryType;

/**
 * This class represents a journal entry for addition of a new mount point.
 */
public class AddMountPointEntry extends JournalEntry {
  private final String mTachyonPath;
  private final String mUfsPath;

  /**
   * Creates a new instance of {@link AddMountPointEntry}.
   *
   * @param tachyonPath the Tachyon path
   * @param ufsPath the UFS path
   */
  @JsonCreator
  public AddMountPointEntry(
      @JsonProperty("tachyonPath") TachyonURI tachyonPath,
      @JsonProperty("ufsPath") TachyonURI ufsPath) {
    mTachyonPath = tachyonPath.toString();
    mUfsPath = ufsPath.toString();
  }

  /**
   * @return the Tachyon path
   */
  @JsonIgnore
  public TachyonURI getTachyonURI() {
    return new TachyonURI(mTachyonPath);
  }

  /**
   * @return the UFS path
   */
  @JsonIgnore
  public TachyonURI getUfsURI() {
    return new TachyonURI(mUfsPath);
  }

  /**
   * @return the Tachyon path
   */
  @JsonGetter
  public String getTachyonPath() {
    return mTachyonPath;
  }

  /**
   * @return the UFS path
   */
  @JsonGetter
  public String getUfsPath() {
    return mUfsPath;
  }

  @Override
  public JournalEntryType getType() {
    return JournalEntryType.ADD_MOUNTPOINT;
  }
}
