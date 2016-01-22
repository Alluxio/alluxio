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
import tachyon.thrift.SetAclTOptions;

/**
 * Method option for setting the acl.
 */
@PublicApi
public class SetAclOptions {
  private String mOwner;
  private String mGroup;
  private short mPermission;
  private boolean mRecursive;

  /**
   * @return the default settings of the acl
   */
  public static SetAclOptions defaults() {
    return new SetAclOptions();
  }

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
  }

  private SetAclOptions() {
    mOwner = null;
    mGroup = null;
    mPermission = Constants.INVALID_PERMISSION;
    mRecursive = false;
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
   * Sets the owner of a path.
   *
   * @param owner to be set as the owner of a path
   * @return the modified options object
   */
  public SetAclOptions setOwner(String owner) {
    mOwner = owner;
    return this;
  }

  /**
   * Sets the group of a path.
   *
   * @param group to be set as the group of a path
   * @return the modified options object
   */
  public SetAclOptions setGroup(String group) {
    mGroup = group;
    return this;
  }

  /**
   * Sets the permission of a path.
   *
   * @param permission to be set as the permission of a path
   * @return the modified options object
   */
  public SetAclOptions setPermission(short permission) {
    mPermission = permission;
    return this;
  }

  /**
   * Sets the recursive flag.
   *
   * @param recursive whether to set acl recursively under a directory
   * @return the modified options object
   */
  public SetAclOptions setRecursive(boolean recursive) {
    mRecursive = recursive;
    return this;
  }

  /**
   * @return Thrift representation of the options
   */
  public SetAclTOptions toThrift() {
    SetAclTOptions options = new SetAclTOptions();
    if (mOwner != null) {
      options.setOwner(mOwner);
    }
    if (mGroup != null) {
      options.setGroup(mGroup);
    }
    if (mPermission != Constants.INVALID_PERMISSION) {
      options.setPermission(mPermission);
    }
    options.setRecursive(mRecursive);
    return options;
  }

  /**
   * @return the options in a readable format
   */
  public String toString() {
    StringBuilder sb = new StringBuilder("SetAclOptions(");
    sb.append(super.toString())
        .append(", Owner: ").append(mOwner)
        .append(", Group: ").append(mGroup)
        .append(", Permission: ").append(mPermission)
        .append(", Recursive: ").append(mRecursive);
    sb.append(")");
    return sb.toString();
  }
}
