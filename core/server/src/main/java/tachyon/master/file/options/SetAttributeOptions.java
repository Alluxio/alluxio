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

import javax.annotation.concurrent.NotThreadSafe;

import tachyon.conf.TachyonConf;
import tachyon.master.MasterContext;
import tachyon.thrift.SetAttributeTOptions;

/**
 * Method option for setting the attributes.
 */
@NotThreadSafe
public class SetAttributeOptions {
  /**
   * Builder for {@link SetAttributeOptions}
   */
  public static class Builder {
    private Boolean mPinned;
    private Long mTtl;
    private Boolean mPersisted;
    private long mOperationTimeMs;

    /**
     * Creates a new builder for {@link SetAttributeOptions}.
     */
    public Builder() {
      this(MasterContext.getConf());
    }

    /**
     * Creates a new builder for {@link SetAttributeOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mPinned = null;
      mTtl = null;
      mPersisted = null;
      mOperationTimeMs = System.currentTimeMillis();
    }

    /**
     * @param pinned the pinned flag value to use
     * @return the builder
     */
    public Builder setPinned(boolean pinned) {
      mPinned = pinned;
      return this;
    }

    /**
     * @param ttl the time-to-live (in seconds) to use
     * @return the builder
     */
    public Builder setTtl(long ttl) {
      mTtl = ttl;
      return this;
    }

    /**
     * @param persisted the persisted flag value to use
     * @return the builder
     */
    public Builder setPersisted(boolean persisted) {
      mPersisted = persisted;
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
     * Builds a new instance of {@link SetAttributeOptions}.
     *
     * @return a {@link SetAttributeOptions} instance
     */
    public SetAttributeOptions build() {
      return new SetAttributeOptions(this);
    }
  }

  private final Boolean mPinned;
  private final Long mTtl;
  private final Boolean mPersisted;
  private long mOperationTimeMs;

  /**
   * Constructs a new method option for setting the attributes.
   *
   * @param options the options for setting the attributes
   */
  public SetAttributeOptions(SetAttributeTOptions options) {
    mPinned = options.isSetPinned() ? options.isPinned() : null;
    mTtl = options.isSetTtl() ? options.getTtl() : null;
    mPersisted = options.isSetPersisted() ? options.isPersisted() : null;
    mOperationTimeMs = System.currentTimeMillis();
  }

  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new Builder(MasterContext.getConf()).build();
  }

  private SetAttributeOptions(Builder builder) {
    mPinned = builder.mPinned;
    mTtl = builder.mTtl;
    mPersisted = builder.mPersisted;
    mOperationTimeMs = builder.mOperationTimeMs;
  }

  /**
   * @return the pinned flag value
   */
  public Boolean getPinned() {
    return mPinned;
  }

  /**
   * @return the time-to-live (in seconds)
   */
  public Long getTtl() {
    return mTtl;
  }

  /**
   * @return the recursive flag value
   */
  public Boolean getPersisted() {
    return mPersisted;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }
}
