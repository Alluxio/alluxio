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
   * @param filesystem     - FileSystem
   * @param conf           - Configuration
   * @param fuseFileSystem - FuseFileSystem
   * @return AuthPolicy
   */
  public static AuthPolicy create(FileSystem filesystem,
      AlluxioConfiguration conf,
      AbstractFuseFileSystem fuseFileSystem) {
    Class authPolicyClazz = conf.getClass(PropertyKey.FUSE_AUTH_POLICY_CLASS);
    try {
      return (AuthPolicy) authPolicyClazz.getConstructor(
          new Class[] {FileSystem.class, AlluxioConfiguration.class,
              AbstractFuseFileSystem.class})
          .newInstance(filesystem, conf, fuseFileSystem);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          PropertyKey.FUSE_AUTH_POLICY_CLASS.getName() + " configured to "
              + authPolicyClazz + ", cannot be constructed.", e);
    }
  }
}
