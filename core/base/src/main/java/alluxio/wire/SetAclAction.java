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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Represents the possible actions for setting ACL.
 */
@PublicApi
@ThreadSafe
public enum SetAclAction {

  /** The specified ACL entries will fully replace the existing ACL. */
  REPLACE,

  /** The specified ACL entries added. */
  MODIFY,

  /** The specified ACL entries will be removed. */
  REMOVE,

  /** All existing ACL entries will be removed, except for the base ACL entries. */
  REMOVE_ALL,

  /** The default ACL entries will be removed. */
  REMOVE_DEFAULT,
  ;
}
