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

package alluxio.underfs;

import alluxio.AlluxioURI;

/**
 * A class that manages the UFS sessions by different services.
 */
public interface UfsSessionManager {
  /**
   * Open a session under the given mount id.
   *
   * @param ufsMountUri the ufs mount point uri
   */
  void openSession(AlluxioURI ufsMountUri);

  /**
   * Close a session under the given mount id.
   *
   * @param ufsMountUri the ufs mount point uri
   */
  void closeSession(AlluxioURI ufsMountUri);
}
