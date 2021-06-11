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

import alluxio.client.file.FileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.jnifuse.AbstractFuseFileSystem;

/**
 * Fuse Auth Policy Factory.
 */
public class AuthPolicyFactory {

  /**
   * Creates a new instance of {@link AuthPolicy}.
   *
   * @param fileSystem     the Alluxio file system
   * @param conf           alluxio configuration
   * @param fuseFileSystem the FuseFileSystem
   * @return AuthPolicy
   */
  public static AuthPolicy create(FileSystem fileSystem,
      AlluxioConfiguration conf,
      AbstractFuseFileSystem fuseFileSystem) {
    Class authPolicyClazz = conf.getClass(PropertyKey.FUSE_AUTH_POLICY_CLASS);
    try {
      return (AuthPolicy) authPolicyClazz.getConstructor(
          new Class[] {FileSystem.class, AlluxioConfiguration.class,
              AbstractFuseFileSystem.class})
          .newInstance(fileSystem, conf, fuseFileSystem);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          PropertyKey.FUSE_AUTH_POLICY_CLASS.getName() + " configured to invalid policy "
              + authPolicyClazz + ". Cannot create authenticate policy.", e);
    }
  }
}
