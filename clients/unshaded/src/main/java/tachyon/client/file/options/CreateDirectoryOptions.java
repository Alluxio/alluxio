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

package tachyon.client.file.options;

import tachyon.Constants;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.UnderStorageType;
import tachyon.client.WriteType;
import tachyon.thrift.CreateDirectoryTOptions;

@PublicApi
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
   * @return the updated options
   */
  public CreateDirectoryOptions setAllowExists(boolean allowExists) {
    mAllowExists = allowExists;
    return this;
  }

  /**
   * @param recursive the recursive flag value to use; it specifies whether parent directories
   *        should be created if they do not already exist
   * @return the updated options
   */
  public CreateDirectoryOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @param writeType the write type to use
   * @return the updated options
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
    StringBuilder sb = new StringBuilder("CreateDirectoryOptions(");
    sb.append(super.toString()).append(", AllowExists: ").append(mAllowExists);
    sb.append(super.toString()).append(", Recursive: ").append(mRecursive);
    sb.append(", UnderStorageType: ").append(mUnderStorageType.toString());
    sb.append(")");
    return sb.toString();
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
