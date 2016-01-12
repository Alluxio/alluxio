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
import tachyon.exception.PreconditionMessage;
import tachyon.thrift.SetStateTOptions;

/**
 * Method options for setting any number of a path's attributes. If a value is set as null, it
 * will be interpreted as an unset value and the current value will be unchanged.
 */
@PublicApi
public final class SetAttributeOptions {
  private Boolean mPinned;
  private Long mTTL;
  private Boolean mPersisted;

  /**
   * @return the default {@link SetAttributeOptions}
   */
  public static SetAttributeOptions defaults() {
    return new SetAttributeOptions();
  }

  /**
   * @param options the thrift options to convert from
   * @return a {@link SetAttributeOptions} logically equivalent to the given thrift options
   */
  public static SetAttributeOptions fromThriftOptions(SetStateTOptions options) {
    return new SetAttributeOptions(options);
  }

  private SetAttributeOptions(SetStateTOptions options) {
    mPinned = options.isSetPinned() ? options.isPinned() : null;
    mTTL = options.isSetTtl() ? options.getTtl() : null;
    mPersisted = options.isSetPersisted() ? options.isPersisted() : null;
  }

  private SetAttributeOptions() {
    mPinned = null;
    mTTL = null;
    mPersisted = null;
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
   * @param pinned the pinned flag value to use; it specifies whether the object should be kept in
   *        memory, if ttl(time to live) is set, the file will be deleted after expiration no
   *        matter this value is true or false
   * @return the updated options object
   */
  public SetAttributeOptions setPinned(boolean pinned) {
    mPinned = pinned;
    return this;
  }

  /**
   * @param ttl the TTL (time to live) value to use; it identifies duration (in milliseconds) the
   *        created file should be kept around before it is automatically deleted, irrespective of
   *        whether the file is pinned
   * @return the updated options object
   */
  public SetAttributeOptions setTTL(long ttl) {
    mTTL = ttl;
    return this;
  }

  /**
   * @param persisted the persisted flag value to use; it specifies whether the file has been
   *        persisted in the under file system or not.
   * @return the updated options object
   */
  public SetAttributeOptions setPersisted(boolean persisted) {
    mPersisted = persisted;
    return this;
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
    sb.append(super.toString()).append(", Pinned: ").append(mPinned).append(", TTL: ").append(mTTL)
        .append(", Persisted: ").append(mPersisted);
    sb.append(")");
    return sb.toString();
  }
}
