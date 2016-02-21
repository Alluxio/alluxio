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

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.master.MasterContext;
import alluxio.thrift.SetAttributeTOptions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Method option for setting the attributes.
 */
@NotThreadSafe
public class SetAttributeOptions {
  /**
   * Builder for {@link SetAttributeOptions}.
   */
  public static class Builder {
    private Boolean mPinned;
    private Long mTtl;
    private Boolean mPersisted;
    private String mOwner;
    private String mGroup;
    private Short mPermission;
    private boolean mRecursive;
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
     * @param conf an Alluxio configuration
     */
    public Builder(Configuration conf) {
      mPinned = null;
      mTtl = null;
      mPersisted = null;
      mOwner = null;
      mGroup = null;
      mPermission = Constants.INVALID_PERMISSION;
      mRecursive = false;
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
     * @param owner the owner to use
     * @return the builder
     */
    public Builder setOwner(String owner) {
      mOwner = owner;
      return this;
    }

    /**
     * @param group the group to use
     * @return the builder
     */
    public Builder setGroup(String group) {
      mGroup = group;
      return this;
    }

    /**
     * @param permission the permission bits to use
     * @return the builder
     */
    public Builder setPermission(short permission) {
      mPermission = permission;
      return this;
    }

    /**
     * @param recursive whether owner / group / permission should be updated recursively
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
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
  private final String mOwner;
  private final String mGroup;
  private final Short mPermission;
  private final boolean mRecursive;
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
    mOwner = options.isSetOwner() ? options.getOwner() : null;
    mGroup = options.isSetGroup() ? options.getGroup() : null;
    mPermission =
        options.isSetPermission() ? options.getPermission() : Constants.INVALID_PERMISSION;
    mRecursive = options.isRecursive();
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
    mOwner = builder.mOwner;
    mGroup = builder.mGroup;
    mPermission = builder.mPermission;
    mRecursive = builder.mRecursive;
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
   * @return the persisted flag value
   */
  public Boolean getPersisted() {
    return mPersisted;
  }

  /**
   * @return the owner
   */
  public String getOwner() {
    return mOwner;
  }

  /**
   * @return the group
   */
  public String getGroup() {
    return mGroup;
  }

  /**
   * @return the permission bits
   */
  public Short getPermission() {
    return mPermission;
  }

  /**
   * @return the recursive flag value
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the operation time (in milliseconds)
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }
}
