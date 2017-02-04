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

package alluxio.master.file.options;

import alluxio.thrift.DeleteTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for deleting a file or a directory.
 */
@NotThreadSafe
public final class DeleteFileOptions {
  private boolean mRecursive;
  private boolean mAlluxioOnly;

  /**
   * @return the default {@link DeleteFileOptions}
   */
  public static DeleteFileOptions defaults() {
    return new DeleteFileOptions();
  }

  /**
   * @param options the {@link DeleteTOptions} to use
   */
  public DeleteFileOptions(DeleteTOptions options) {
    mRecursive = options.isRecursive();
    mAlluxioOnly = options.isAlluxioOnly();
  }

  private DeleteFileOptions() {
    mRecursive = false;
    mAlluxioOnly = false;
  }

  /**
   * @return the recursive flag value; if the object to be deleted is a directory, the flag
   *         specifies whether the directory content should be recursively deleted as well
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the alluxioOnly flag value; the flag specifies whether the file content should
   *         be removed from Alluxio, but not from UFS.
   */
  public boolean isAlluxioOnly() {
    return mAlluxioOnly;
  }

  /**
   * @param recursive the recursive flag value to use; if the object to be deleted is a directory,
   *        the flag specifies whether the directory content should be recursively deleted as well
   * @return the updated options object
   */
  public DeleteFileOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param removeUFSFile the removeUFSFile flag value to use; the flag specifies whether the file
   *        content should be removed from Alluxio, but not from UFS.
   * @return the updated options object
   */
  public DeleteFileOptions setAlluxioOnly(boolean removeUFSFile) {
    mAlluxioOnly = removeUFSFile;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeleteFileOptions)) {
      return false;
    }
    DeleteFileOptions that = (DeleteFileOptions) o;
    return Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mAlluxioOnly, that.mAlluxioOnly);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecursive, mAlluxioOnly);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
         .add("recursive", mRecursive)
         .add("alluxioOnly", mAlluxioOnly)
         .toString();
  }
}
