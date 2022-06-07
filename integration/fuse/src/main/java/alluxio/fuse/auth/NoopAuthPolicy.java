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
import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.jnifuse.AbstractFuseFileSystem;

/**
 * A Fuse authentication policy that does nothing.
 * Mainly for testing purpose.
 */
public class NoopAuthPolicy implements AuthPolicy {

  /**
   * @param fileSystem     the Alluxio file system
   * @param conf           alluxio configuration
   * @param fuseFileSystem the FuseFileSystem
   */
  public NoopAuthPolicy(FileSystem fileSystem, AlluxioConfiguration conf,
      AbstractFuseFileSystem fuseFileSystem) {
  }

  @Override
  public void setUserGroupIfNeeded(AlluxioURI uri) {}
}
