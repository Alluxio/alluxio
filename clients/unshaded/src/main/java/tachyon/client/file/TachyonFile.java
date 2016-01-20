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

package tachyon.client.file;

/**
 * A file handler for a file in Tachyon. It is a wrapper around the file ID for now.
 */
public class TachyonFile {
  private final long mFileId;

  /**
   * Creates a new Tachyon file.
   *
   * @param fileId the file id
   */
  public TachyonFile(long fileId) {
    mFileId = fileId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TachyonFile)) {
      return false;
    }
    TachyonFile that = (TachyonFile) o;
    return mFileId == that.mFileId;
  }

  /**
   * @return the file id
   */
  public long getFileId() {
    return mFileId;
  }

  @Override
  public int hashCode() {
    return Long.valueOf(mFileId).hashCode();
  }

  @Override
  public String toString() {
    return "TachyonFile(" + mFileId + ")";
  }
}
