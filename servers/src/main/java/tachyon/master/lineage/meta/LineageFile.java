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

public final class LineageFile extends TachyonFile {
  private LineageFileState mState;
  private String mUnderFilePath;

  public LineageFile(long fileId) {
    super(fileId);
    mState = LineageFileState.CREATED;
  }

  public String getUnderFilePath() {
    return mUnderFilePath;
  }

  public void setUnderFilePath(String path) {
    mUnderFilePath = path;
  }

  public LineageFileState getState() {
    return mState;
  }

  public void setState(LineageFileState state) {
    mState = state;
  }

  @Override
  public String toString() {
    return "Lineage File(mState:" + mState + ",mUnderFilePath" + mUnderFilePath + ")";
  }
}
