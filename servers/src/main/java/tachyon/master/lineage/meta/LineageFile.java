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

package tachyon.master.lineage.meta;

import tachyon.client.file.TachyonFile;
import tachyon.thrift.LineageFileInfo;

/**
 * A file declared as a lineage's output.
 */
public final class LineageFile extends TachyonFile {
  private LineageFileState mState;

  /**
   * Constructs the lineage file with the default state as created.
   *
   * @param fileId the file id
   */
  public LineageFile(long fileId) {
    super(fileId);
    mState = LineageFileState.CREATED;
  }

  /**
   * Constructs the lineage file.
   *
   * @param fileId the file id
   * @param state the state
   */
  public LineageFile(long fileId, LineageFileState state) {
    super(fileId);
    mState = state;
  }

  /**
   * @return the generated {@link LineageFileInfo} for RPC
   */
  public LineageFileInfo generateLineageFileInfo() {
    LineageFileInfo info = new LineageFileInfo();
    info.mId = getFileId();
    info.mState = mState.toString();
    return info;
  }

  /**
   * @return the file state
   */
  public LineageFileState getState() {
    return mState;
  }

  /**
   * Sets the file state.
   *
   * @param state the new state
   */
  public void setState(LineageFileState state) {
    mState = state;
  }

  @Override
  public String toString() {
    return "Lineage File(mId:" + getFileId() + ", mState:" + mState + ")";
  }
}
