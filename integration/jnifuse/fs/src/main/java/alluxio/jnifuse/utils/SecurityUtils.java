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

package alluxio.jnifuse.utils;

public final class SecurityUtils {
  public static boolean canHandleShutdownHooks() {
    SecurityManager security = System.getSecurityManager();
    if (security == null) {
      return true;
    }
    try {
      security.checkPermission(new RuntimePermission("shutdownHooks"));
      return true;
    } catch (final SecurityException e) {
      return false;
    }
  }

  private SecurityUtils() {} // prevent instantiation
}
