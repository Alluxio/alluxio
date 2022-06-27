/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.fuse.auth;

import alluxio.AlluxioURI;

/**
 * Fuse Auth Policy Interface.
 */
public interface AuthPolicy {
  /**
   * Sets user and group.
   *
   * @param uri the path uri
   */
  void setUserGroup(AlluxioURI uri);

  /**
   * Sets user and group.
   *
   * @param uri the path uri
   * @param uid the user id to set
   * @param gid the gid to set
   */
  void setUserGroup(AlluxioURI uri, long uid, long gid);

  /**
   * Gets the uid based on the auth policy and file owner.
   *
   * @param owner the owner of the file
   * @return the uid
   */
  long getUid(String owner);

  /**
   * Gets the gid based on the auth policy and file group.
   *
   * @param group the file group
   * @return the gid
   */
  long getGid(String group);
}
