/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.file.options;

import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
import alluxio.client.UnderStorageType;
import alluxio.client.WriteType;
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
    WriteType defaultWriteType =
        ClientContext.getConf().getEnum(Constants.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class);
    mUnderStorageType = defaultWriteType.getUnderStorageType();
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

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("allowExists", mAllowExists)
        .add("recursive", mRecursive).add("underStorageType", mUnderStorageType).toString();
  }

  /**
   * @return Thrift representation of the options
   */
  public CreateDirectoryTOptions toThrift() {
    CreateDirectoryTOptions options = new CreateDirectoryTOptions();
    options.setAllowExists(mAllowExists);
    options.setRecursive(mRecursive);
    options.setPersisted(mUnderStorageType.isSyncPersist());
    return options;
  }
}
