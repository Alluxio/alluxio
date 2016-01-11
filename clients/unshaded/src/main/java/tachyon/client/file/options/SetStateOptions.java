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

import com.google.common.base.Preconditions;

import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.exception.PreconditionMessage;
import tachyon.thrift.SetStateTOptions;

/**
 * Method option for setting the state.
 */
@PublicApi
public class SetStateOptions {

  /**
   * Builder for {@link SetStateOptions}.
   */
  public static class Builder implements OptionsBuilder<SetStateOptions> {
    private Boolean mPinned;
    private Long mTtl;
    private Boolean mPersisted;

    /**
     * Creates a new builder for {@link SetStateOptions}.
     */
    public Builder() {
      this(ClientContext.getConf());
    }

    /**
     * Creates a new builder for {@link SetStateOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mPinned = null;
      mTtl = null;
      mPersisted = null;
    }

    /**
     * Sets the pinned flag.
     *
     * @param pinned the pinned flag value to use; it specifies whether the object should be kept in
     *        memory, if ttl(time to live) is set, the file will be deleted after expiration no
     *        matter this value is true or false
     * @return the builder
     */
    public Builder setPinned(boolean pinned) {
      mPinned = pinned;
      return this;
    }

    /**
     * Sets the time to live.
     *
     * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
     *        created file should be kept around before it is automatically deleted, irrespective of
     *        whether the file is pinned
     * @return the builder
     */
    public Builder setTtl(long ttl) {
      mTtl = ttl;
      return this;
    }

    /**
     * Sets the persisted flag.
     *
     * @param persisted the persisted flag value to use; it specifies whether the file has been
     *        persisted in the under file system or not.
     * @return the builder
     */
    public Builder setPersisted(boolean persisted) {
      mPersisted = persisted;
      return this;
    }

    /**
     * Builds a new instance of {@link SetStateOptions}.
     *
     * @return a {@link SetStateOptions} instance
     */
    @Override
    public SetStateOptions build() {
      return new SetStateOptions(this);
    }
  }

  /**
   * @return the default {@link SetStateOptions}
   */
  public static SetStateOptions defaults() {
    return new Builder().build();
  }

  private final Boolean mPinned;
  private final Long mTtl;
  private final Boolean mPersisted;

  /**
   * Constructs a new method option for setting the state.
   *
   * @param options the options for setting the state
   */
  public SetStateOptions(SetStateTOptions options) {
    mPinned = options.isSetPinned() ? options.isPinned() : null;
    mTtl = options.isSetTtl() ? options.getTtl() : null;
    mPersisted = options.isSetPersisted() ? options.isPersisted() : null;
  }

  private SetStateOptions(SetStateOptions.Builder builder) {
    mPinned = builder.mPinned;
    mTtl = builder.mTtl;
    mPersisted = builder.mPersisted;
  }

  /**
   * @return true if the pinned flag is set, otherwise false
   */
  public boolean hasPinned() {
    return mPinned != null;
  }

  /**
   * @return the pinned flag value; it specifies whether the object should be kept in memory
   */
  public boolean getPinned() {
    Preconditions.checkState(hasPinned(), PreconditionMessage.MUST_SET_PINNED);
    return mPinned;
  }

  /**
   * @return true if the TTL value is set, otherwise false
   */
  public boolean hasTtl() {
    return mTtl != null;
  }

  /**
   * @return the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *         created file should be kept around before it is automatically deleted, irrespective of
   *         whether the file is pinned
   */
  public long getTtl() {
    Preconditions.checkState(hasTtl(), PreconditionMessage.MUST_SET_TTL);
    return mTtl;
  }

  /**
   * @return true if the persisted value is set, otherwise false
   */
  public boolean hasPersisted() {
    return mPersisted != null;
  }

  /**
   * @return the persisted value of the file; it denotes whether the file has been persisted to the
   *         under file system or not.
   */
  public boolean getPersisted() {
    Preconditions.checkState(hasPersisted(), PreconditionMessage.MUST_SET_PERSISTED);
    return mPersisted;
  }

  /**
   * @return Thrift representation of the options
   */
  public SetStateTOptions toThrift() {
    SetStateTOptions options = new SetStateTOptions();
    if (mPinned != null) {
      options.setPinned(mPinned);
    }
    if (mTtl != null) {
      options.setTtl(mTtl);
    }
    if (mPersisted != null) {
      options.setPersisted(mPersisted);
    }
    return options;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SetStateOptions(");
    sb.append(super.toString()).append(", Pinned: ").append(mPinned).append(", TTL: ").append(mTtl)
        .append(", Persisted: ").append(mPersisted);
    sb.append(")");
    return sb.toString();
  }
}
