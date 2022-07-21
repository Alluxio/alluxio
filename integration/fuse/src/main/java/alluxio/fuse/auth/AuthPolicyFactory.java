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
import alluxio.conf.PropertyKey;
import alluxio.fuse.AlluxioFuseFileSystemOpts;
import alluxio.jnifuse.FuseFileSystem;

import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Fuse Auth Policy Factory.
 */
public class AuthPolicyFactory {

  /**
   * Creates a new instance of {@link AuthPolicy}.
   *
   * @param fileSystem     the Alluxio file system
   * @param fuseFsOpts     the options for AlluxioFuse filesystem
   * @param fuseFileSystem the FuseFileSystem
   * @return AuthPolicy
   */
  public static AuthPolicy create(FileSystem fileSystem,
      AlluxioFuseFileSystemOpts fuseFsOpts,
      FuseFileSystem fuseFileSystem) {
    Class authPolicyClazz = fuseFsOpts.getFuseAuthPolicyClass();
    try {
      Method createMethod = authPolicyClazz.getMethod("create",
          FileSystem.class, AlluxioFuseFileSystemOpts.class,
          Optional.class);
      AuthPolicy authPolicy = (AuthPolicy) createMethod
          .invoke(null, fileSystem, fuseFsOpts, Optional.of(fuseFileSystem));
      authPolicy.init();
      return authPolicy;
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          PropertyKey.FUSE_AUTH_POLICY_CLASS.getName() + " configured to invalid policy "
              + authPolicyClazz + ". Cannot create authenticate policy.", e);
    }
  }
}
