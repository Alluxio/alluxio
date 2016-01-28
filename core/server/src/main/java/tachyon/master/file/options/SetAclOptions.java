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

import tachyon.Constants;
import tachyon.conf.TachyonConf;
import tachyon.exception.ExceptionMessage;
import tachyon.master.MasterContext;
import tachyon.thrift.SetAclTOptions;

/**
 * Method option for setting the acl.
 */
@NotThreadSafe
public class SetAclOptions {

  /**
   * Builder for {@link SetAclOptions}
   */
  public static class Builder {
    private String mOwner;
    private String mGroup;
    private short mPermission;
    private boolean mRecursive;
    private long mOperationTimeMs;

    /**
     * Creates a new builder for {@link SetAclOptions}.
     */
    public Builder() {
      this(MasterContext.getConf());
    }

    /**
     * Creates a new builder for {@link SetAclOptions}.
     *
     * @param conf a Tachyon configuration
     */
    public Builder(TachyonConf conf) {
      mOwner = null;
      mGroup = null;
      mPermission = Constants.INVALID_PERMISSION;
      mRecursive = false;
      mOperationTimeMs = System.currentTimeMillis();
    }

    /**
     * Sets the owner of a path.
     *
     * @param owner to be set as the owner of a path
     * @return the builder
     */
    public Builder setOwner(String owner) {
      mOwner = owner;
      return this;
    }

    /**
     * Sets the group of a path.
     *
     * @param group to be set as the group of a path
     * @return the builder
     */
    public Builder setGroup(String group) {
      mGroup = group;
      return this;
    }

    /**
     * Sets the permission of a path.
     *
     * @param permission to be set as the permission of a path
     * @return the builder
     */
    public Builder setPermission(short permission) {
      mPermission = permission;
      return this;
    }

    /**
     * Sets the recursive flag.
     *
     * @param recursive whether to set acl recursively under a directory
     * @return the builder
     */
    public Builder setRecursive(boolean recursive) {
      mRecursive = recursive;
      return this;
    }

    /**
     * Sets the operation time.
     *
     * @param operationTimeMs the operation time to use
     * @return the builder
     */
    public Builder setOperationTimeMs(long operationTimeMs) {
      mOperationTimeMs = operationTimeMs;
      return this;
    }

    /**
     * Builds a new instance of {@link SetAclOptions}.
     *
     * @return a {@link SetAclOptions} instance
     */
    public SetAclOptions build() {
      SetAclOptions options = new SetAclOptions(this);
      if (options.isValid()) {
        return options;
      }
      throw new IllegalArgumentException(
          ExceptionMessage.INVALID_SET_ACL_OPTIONS.getMessage(mOwner, mGroup, mPermission));
    }
  }

  private final String mOwner;
  private final String mGroup;
  private final short mPermission;
  private final boolean mRecursive;
  private long mOperationTimeMs;

  /**
   * Constructs a new method option for setting the acl.
   *
   * @param options the options for setting the acl
   */
  public SetAclOptions(SetAclTOptions options) {
    mOwner = options.isSetOwner() ? options.getOwner() : null;
    mGroup = options.isSetGroup() ? options.getGroup() : null;
    mPermission =
        options.isSetPermission() ? (short) options.getPermission() : Constants.INVALID_PERMISSION;
    mRecursive = options.isSetRecursive() ? options.isRecursive() : null;
    mOperationTimeMs = System.currentTimeMillis();
  }

  private SetAclOptions(Builder builder) {
    mOwner = builder.mOwner;
    mGroup = builder.mGroup;
    mPermission = builder.mPermission;
    mRecursive = builder.mRecursive;
    mOperationTimeMs = builder.mOperationTimeMs;
  }

  /**
   * Checks whether the instance of {@link SetAclOptions} is valid,
   * which means at least one of three attributes (owner, group, permission) takes effect.
   *
   * @return true if the instance of {@link SetAclOptions} is valid, false otherwise
   */
  public boolean isValid() {
    return mOwner != null || mGroup != null || mPermission != Constants.INVALID_PERMISSION;
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
   * @return the permission
   */
  public short getPermission() {
    return mPermission;
  }

  /**
   * @return the recursive flag value
   */
  public boolean isRecursive() {
    return mRecursive;
  }

  /**
   * @return the operation time
   */
  public long getOperationTimeMs() {
    return mOperationTimeMs;
  }
}
