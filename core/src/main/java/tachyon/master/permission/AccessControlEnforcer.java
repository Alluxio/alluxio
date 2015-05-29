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

package tachyon.master.permission;

import tachyon.master.Inode;
import tachyon.master.permission.AclEntry.AclPermission;
import tachyon.security.UserGroup;
import tachyon.thrift.AccessControlException;

public interface AccessControlEnforcer {
  /**
   * Checks permission on a file system object. Has to throw an Exception
   * if the filesystem object is not accessessible by the calling Authenticator.
   * @param fsOwner Tachyon file system owner (The Tachyon master user)
   * @param supergroup super group
   * @param caller The caller of check permissions
   * @param inodes Array of INodes for each path element in the path
   * @param pathByNameArr Array of byte arrays of the LocalName
   * @param path Path string
   * @param ancestorIndex Index of ancestor
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess The access required by the parent of the path.
   * @param access The access required by the path.
   * @param doCheckOwner perform ownership check
   * @throws AccessControlException
   */
  public abstract void checkPermission(String fsOwner, String supergroup,
      UserGroup caller, Inode[] inodes, String[] pathByNameArr,
      String path, int ancestorIndex, AclPermission ancestorAccess,
      AclPermission parentAccess, AclPermission access,boolean doCheckOwner)
      throws AccessControlException;
}
