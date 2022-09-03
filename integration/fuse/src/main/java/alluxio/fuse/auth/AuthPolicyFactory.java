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

import java.lang.reflect.InvocationTargetException;
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
    Class<?> authPolicyClazz = fuseFsOpts.getFuseAuthPolicyClass();
    if (!AuthPolicy.class.isAssignableFrom(authPolicyClazz)) {
      throw new IllegalArgumentException(String.format(
          "Cannot configure %s to %s, policy description: %s",
          PropertyKey.FUSE_AUTH_POLICY_CLASS.getName(), authPolicyClazz,
          PropertyKey.FUSE_AUTH_POLICY_CLASS.getDescription()));
    }
    try {
      Class<? extends AuthPolicy> subAuthPolicyClazz = fuseFsOpts.getFuseAuthPolicyClass()
          .asSubclass(AuthPolicy.class);
      Method createMethod = subAuthPolicyClazz.getMethod("create",
          FileSystem.class, AlluxioFuseFileSystemOpts.class,
          Optional.class);
      AuthPolicy authPolicy = (AuthPolicy) createMethod
          .invoke(null, fileSystem, fuseFsOpts, Optional.of(fuseFileSystem));
      authPolicy.init();
      return authPolicy;
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(String.format(
          "Cannot configure %s to %s, policy description: %s",
          PropertyKey.FUSE_AUTH_POLICY_CLASS.getName(), authPolicyClazz,
          PropertyKey.FUSE_AUTH_POLICY_CLASS.getDescription()), e);
    } catch (NoSuchMethodException | IllegalArgumentException
        | IllegalAccessException | InvocationTargetException ne) {
      throw new IllegalArgumentException(
          String.format("Failed to create %s: should not be reached here",
              authPolicyClazz.getName()), ne);
    }
  }
}
