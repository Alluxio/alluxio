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

package alluxio.master.file.options;

import javax.annotation.concurrent.NotThreadSafe;

import alluxio.Configuration;
import alluxio.master.MasterContext;
import alluxio.thrift.CreateDirectoryTOptions;

/**
 * Method option for creating a directory.
 */
@NotThreadSafe
public final class CreateDirectoryOptions {
  /**
   * Builder for {@link CreateDirectoryOptions}.
   */
  public static class Builder {
    private boolean mAllowExists;
    private long mOperationTimeMs;
    private boolean mPersisted;
    private boolean mRecursive;

    /**
     * Creates a new builder for {@link CreateDirectoryOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(Configuration conf) {
      mOperationTimeMs = System.currentTimeMillis();
      mPersisted = false;
      mRecursive = false;
    }

    /**
     * @param allowExists the allowExists flag value to use; it specifies whether an exception
     *        should be thrown if the directory being made already exists.
     * @return the builder
     */
    public Builder setAllowExists(boolean allowExists) {
      mAllowExists = allowExists;
      return this;
    }

    /**
     * @param operationTimeMs the operation time to use
     * @return the builder
     */
    public Builder setOperationTimeMs(long operationTimeMs) {
      mOperationTimeMs = operationTimeMs;
      return this;
    }

    /**
     * @param persisted the persisted flag to use; it specifies whether the object to created is
     *        persisted in UFS
     * @return the builder
     */
    public Builder setPersisted(boolean persisted) {
      mPersisted = persisted;
      return this;
    }

    /**
     * @param recursive the recursive flag value to use; it specifies whether parent directories
     *        should be created if they do not already exist
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * Builds a new instance of {@link CreateDirectoryOptions}.
     *
     * @return a {@link CreateDirectoryOptions} instance
     */
    public CreateDirectoryOptions build() {
      return new CreateDirectoryOptions(this);
    }
  }

  /**
   * @return the default {@link CreateDirectoryOptions}
   */
  public static CreateDirectoryOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private boolean mAllowExists;
  private long mOperationTimeMs;
  private boolean mPersisted;
  private boolean mRecursive;

  private CreateDirectoryOptions(CreateDirectoryOptions.Builder builder) {
    mAllowExists = builder.mAllowExists;
    mOperationTimeMs = builder.mOperationTimeMs;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
  }

  /**
   * Creates a new instance of {@link CreateDirectoryOptions} from {@link CreateDirectoryTOptions}.
   *
   * @param options Thrift options
   */
  public CreateDirectoryOptions(CreateDirectoryTOptions options) {
    mAllowExists = options.isAllowExists();
    mOperationTimeMs = System.currentTimeMillis();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
  }

  /**
   * @return the allowExists flag; it specifies whether an exception should be thrown if the
   *         directory being made already exists.
   */
  public boolean isAllowExists() {
    return mAllowExists;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }

  /**
   * @return the persisted flag; it specifies whether the object to create is persisted in UFS
   */
  public boolean isPersisted() {
    return mPersisted;
  }

  /**
   * @return the recursive flag value; it specifies whether parent directories should be created if
   *         they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }
}
