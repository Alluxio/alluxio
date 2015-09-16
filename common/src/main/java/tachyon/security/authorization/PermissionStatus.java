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

package tachyon.security.authorization;

public class PermissionStatus {
  private String mUsername;
  private String mGroupname;
  private FsPermission mPermission;

  public PermissionStatus(String username, String groupname, FsPermission permission) {
    mUsername = username;
    mGroupname = groupname;
    mPermission = permission;
  }

  public PermissionStatus(String username, String groupname, short permission) {
    mUsername = username;
    mGroupname = groupname;
    mPermission = new FsPermission(permission);
  }

  /** Return user name */
  public String getUserName() {
    return mUsername;
  }

  /** Return group name */
  public String getGroupName() {
    return mGroupname;
  }

  /** Return permission */
  public FsPermission getPermission() {
    return mPermission;
  }

  /**
   * Apply umask.
   * @see FsPermission#applyUMask(FsPermission)
   */
  public PermissionStatus applyUMask(FsPermission umask) {
    FsPermission newFsPermission = mPermission.applyUMask(umask);
    return new PermissionStatus(mUsername, mGroupname, newFsPermission);
  }

  /** Get the Directory default PermissionStatus. */
  public static PermissionStatus getDirDefault() {
    return new PermissionStatus("", "", new FsPermission((short)0755));
  }

  /** {@inheritDoc} */
  public String toString() {
    return mUsername + ":" + mGroupname + ":" + mPermission;
  }
}
