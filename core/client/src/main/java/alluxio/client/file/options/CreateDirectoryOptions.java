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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.annotation.PublicApi;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
import alluxio.security.authorization.Mode;
import alluxio.thrift.CreateDirectoryTOptions;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method options for creating a directory.
 */
@PublicApi
@NotThreadSafe
public final class CreateDirectoryOptions {
  private boolean mAllowExists;
  private Mode mMode; // null if creating the dir using system default mode
  private boolean mRecursive;
  private UnderStorageType mUnderStorageType;

  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new CreateDirectoryOptions();
  }

  private CreateDirectoryOptions() {
    mRecursive = false;
    mAllowExists = false;
    mMode = null;
    WriteType defaultWriteType =
        Configuration.getEnum(PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    mUnderStorageType = defaultWriteType.getUnderStorageType();
  }

  /**
   * @return the mode of the directory to create
   */
  public Mode getMode() {
    return mMode;
  }

  /**
   * @return the under storage type
   */
  public UnderStorageType getUnderStorageType() {
    return mUnderStorageType;
  }

  /**
   * @return the allowExists flag value; it specifies whether an exception should be thrown if the
   *         directory being made already exists
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @param allowExists the allowExists flag value to use; it specifies whether an exception
   *        should be thrown if the directory being made already exists.
   * @return the updated options object
   */
  public CreateDirectoryOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  /**
   * @param mode the mode to be set
   * @return the updated options object
   */
  public CreateDirectoryOptions setMode(Mode mode) {
    mMode = mode;
    return this;
  }

  /**
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options object
   */
  public CreateDirectoryOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param writeType the write type to use
   * @return the updated options object
   */
  public CreateDirectoryOptions setWriteType(WriteType writeType) {
    mUnderStorageType = writeType.getUnderStorageType();
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CreateDirectoryOptions)) {
      return false;
    }
    CreateDirectoryOptions that = (CreateDirectoryOptions) o;
    return Objects.equal(mAllowExists, that.mAllowExists)
        && Objects.equal(mMode, that.mMode)
        && Objects.equal(mRecursive, that.mRecursive)
        && Objects.equal(mUnderStorageType, that.mUnderStorageType);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mAllowExists, mMode, mRecursive, mUnderStorageType);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("allowExists", mAllowExists)
        .add("mode", mMode)
        .add("recursive", mRecursive)
        .add("underStorageType", mUnderStorageType)
        .toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateDirectoryTOptions toThrift() {
    CreateDirectoryTOptions options = new CreateDirectoryTOptions();
    options.setAllowExists(mAllowExists);
    options.setRecursive(mRecursive);
    options.setPersisted(mUnderStorageType.isSyncPersist());
    if (mMode != null) {
      options.setMode(mMode.toShort());
    }
    return options;
  }
}
