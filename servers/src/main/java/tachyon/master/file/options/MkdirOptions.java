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

package tachyon.master.file.options;

import tachyon.conf.TachyonConf;
import tachyon.master.MasterContext;
import tachyon.thrift.MkdirTOptions;

public final class MkdirOptions {
  public static class Builder {
    private long mOperationTimeMs;
    private boolean mPersisted;
    private boolean mRecursive;

    /**
     * Creates a new builder for {@link MkdirOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mOperationTimeMs = System.currentTimeMillis();
      mPersisted = false;
      mRecursive = false;
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
     * Builds a new instance of {@link MkdirOptions}.
     *
     * @return a {@link MkdirOptions} instance
     */
    public MkdirOptions build() {
      return new MkdirOptions(this);
    }
  }

  /**
   * @return the default {@link MkdirOptions}
   */
  public static MkdirOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private long mOperationTimeMs;
  private boolean mPersisted;
  private boolean mRecursive;

  private MkdirOptions(MkdirOptions.Builder builder) {
    mOperationTimeMs = builder.mOperationTimeMs;
    mPersisted = builder.mPersisted;
    mRecursive = builder.mRecursive;
  }

  /**
   * Creates a new instance of {@link MkdirOptions} from {@link MkdirTOptions}.
   *
   * @param options Thrift options
   */
  public MkdirOptions(MkdirTOptions options) {
    mOperationTimeMs = System.currentTimeMillis();
    mPersisted = options.isPersisted();
    mRecursive = options.isRecursive();
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
   * they do not already exist
   */
  public boolean isRecursive() {
    return mRecursive;
  }
}
