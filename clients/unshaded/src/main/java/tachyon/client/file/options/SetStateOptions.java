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

import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;
import tachyon.exception.PreconditionMessage;
import tachyon.thrift.SetStateTOptions;

public class SetStateOptions {
  public static class Builder implements OptionsBuilder<SetStateOptions> {
    private Boolean mPinned;
    private Long mTTL;

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
      mTTL = null;
    }

    /**
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
     * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
     *        created file should be kept around before it is automatically deleted, irrespective of
     *        whether the file is pinned
     * @return the builder
     */
    public Builder setTTL(long ttl) {
      mTTL = ttl;
      return this;
    }

    /**
     * Builds a new instance of {@code SetStateOptions}.
     *
     * @return a {@code SetStateOptions} instance
     */
    @Override
    public SetStateOptions build() {
      return new SetStateOptions(this);
    }
  }

  /**
   * @return the default {@code SetStateOptions}
   */
  public static SetStateOptions defaults() {
    return new Builder().build();
  }

  private final Boolean mPinned;
  private final Long mTTL;

  public SetStateOptions(SetStateTOptions options) {
    mPinned = options.isSetPinned() ? options.isPinned() : null;
    mTTL = options.isSetTtl() ? options.getTtl() : null;
  }

  private SetStateOptions(SetStateOptions.Builder builder) {
    mPinned = builder.mPinned;
    mTTL = builder.mTTL;
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
  public boolean hasTTL() {
    return mTTL != null;
  }

  /**
   * @return the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *         created file should be kept around before it is automatically deleted, irrespective of
   *         whether the file is pinned
   */
  public long getTTL() {
    Preconditions.checkState(hasTTL(), PreconditionMessage.MUST_SET_TTL);
    return mTTL;
  }

  /**
   * @return Thrift representation of the options
   */
  public SetStateTOptions toThrift() {
    SetStateTOptions options = new SetStateTOptions();
    if (mPinned != null) {
      options.setPinned(mPinned);
    }
    if (mTTL != null) {
      options.setTtl(mTTL);
    }
    return options;
  }

  /**
   * @return the name : value pairs for all the fields
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("SetStateOptions(");
    sb.append(super.toString()).append(", Pinned: ").append(mPinned)
        .append(", TTL: ").append(mTTL);
    sb.append(")");
    return sb.toString();
  }
}
