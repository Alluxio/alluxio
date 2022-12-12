/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs;

import java.util.Objects;

/**
 * Opaque file identifiers, which are typically a long or a string-based ID.
 * File IDs can be used as keys of a map.
 */
// As the IDs are to be used as keys of a map and needs to be inspected in logs,
// implementors must override equals, hashCode and toString.
public abstract class FileId {
  private FileId() {
    // sealed class
  }

  /**
   * Gets a long-based file ID.
   *
   * @param fileId the file ID
   * @return file ID
   */
  public static FileId of(long fileId) {
    return new LongId(fileId);
  }

  /**
   * Gets a String-based file ID.
   *
   * @param fileId the file ID
   * @return file ID
   */
  public static FileId of(String fileId) {
    return new StringId(fileId);
  }

  static final class StringId extends FileId {
    private final String mFileId;

    public StringId(String fileId) {
      mFileId = Objects.requireNonNull(fileId, "fileId");
    }

    @Override
    public boolean equals(Object obj) {
      return mFileId.equals(obj);
    }

    @Override
    public int hashCode() {
      return mFileId.hashCode();
    }

    @Override
    public String toString() {
      return mFileId;
    }
  }

  static final class LongId extends FileId {
    private final long mFileId;

    public LongId(long fileId) {
      mFileId = fileId;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LongId longId = (LongId) o;
      return mFileId == longId.mFileId;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(mFileId);
    }

    @Override
    public String toString() {
      return String.valueOf(mFileId);
    }
  }
}
