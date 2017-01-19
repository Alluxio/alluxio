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

package alluxio.client.file.options;

import alluxio.annotation.PublicApi;
import alluxio.thrift.DeleteTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for deleting a file.
 */
@PublicApi
@NotThreadSafe
public final class DeleteOptions {
  private boolean mRecursive;
  private boolean mRemoveUFSFile;

  /**
   * @return the default {@link DeleteOptions}
   */
  public static DeleteOptions defaults() {
    return new DeleteOptions();
  }

  private DeleteOptions() {
    mRecursive = false;
    mRemoveUFSFile = true;
  }

  /**
   * @return the recursive flag value; if the object to be deleted is a directory, the flag
   *         specifies whether the directory content should be recursively deleted as well
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the removeUFSFile flag value; the flag specifies whether the file content should
   *         be deleted from the under storage file system.
   */
  public boolean isRemoveUFSFile() {
    return mRemoveUFSFile;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive the recursive flag value to use; if the object to be deleted is a directory,
   *        the flag specifies whether the directory content should be recursively deleted as well
   * @return the updated options object
   */
  public DeleteOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * Sets the removeUFSFile flag.
   *
   * @param removeUFSFile the removeUFSFile flag value to use; the flag specifies whether the file
   *        content should be deleted from the under storage file system.
   * @return the updated options object
   */
  public DeleteOptions setRemoveUFSFile(boolean removeUFSFile) {
    mRemoveUFSFile = removeUFSFile;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DeleteOptions)) {
      return false;
    }
    DeleteOptions that = (DeleteOptions) o;
    return Objects.equal(mRecursive, that.mRecursive) && Objects.equal(mRemoveUFSFile,
        that.mRemoveUFSFile);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecursive, mRemoveUFSFile);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("recursive", mRecursive)
        .add("removeUFSFile", mRemoveUFSFile)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public DeleteTOptions toThrift() {
    DeleteTOptions options = new DeleteTOptions();
    options.setRecursive(mRecursive);
    options.setRemoveUFSFile(mRemoveUFSFile);
    return options;
  }
}
