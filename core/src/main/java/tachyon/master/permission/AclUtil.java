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

import org.apache.hadoop.conf.Configuration;

import tachyon.master.permission.AclEntry.AclPermission;

public class AclUtil {
  private static final AclPermission[] ACL_PERMISSIONS = AclPermission.values();
  // ACL enable key, the default value of this key is true
  public static final String ACL_ENABLE_KEY = "tfs.permission.enable";

  // umask key
  public static final String UMASK_KEY = "tfs.permission.umask";

  // Default value of umask
  public static final int DEFAULT_UMASK = 0022;

  // Default permission of directory
  public static final short DEFAULT_DIR_PERMISSION = 0777;

  // Default permission of file
  public static final short DEFAULT_FILE_PERMISSION = 0666;

  /**
   * Get user permission from a short
   * @param n, short permission, e.g. 777
   * @return user {@link AclPermission}
   */
  public static AclPermission toUserPermission(short n) {
    AclPermission[] v = ACL_PERMISSIONS;
    return v[(n >>> 6) & 7];
  }

  /**
   * Get group permission from a short
   * @param n, short permission, e.g. 777
   * @return group {@link AclPermission}
   */
  public static AclPermission toGroupPermission(short n) {
    AclPermission[] v = ACL_PERMISSIONS;
    return v[(n >>> 3) & 7];
  }

  /**
   * Get other permission from a short
   * @param n, short permission, e.g. 777
   * @return other {@link AclPermission}
   */
  public static AclPermission toOtherPermission(short n) {
    AclPermission[] v = ACL_PERMISSIONS;
    return v[n & 7];
  }

  /**
   * Get permission from a String
   * @param s, a String of permission, e.g. rwx
   * @return a {@link AclPermission} which SYMBOL equal s
   */
  public static AclPermission getPermission(String s) {
    AclPermission[] v = ACL_PERMISSIONS;
    for (AclPermission aclPermission : v) {
      if (aclPermission.mValue.equals(s)) {
        return aclPermission;
      }
    }
    return null;
  }

  /**
   * Get umask from configuration
   * @param conf
   * @return umask
   */
  public static short getUMask(Configuration conf) {
    int umask = DEFAULT_UMASK;
    if (conf != null) {
      umask = conf.getInt(UMASK_KEY, DEFAULT_UMASK);
    }

    return (short)umask;
  }
}
