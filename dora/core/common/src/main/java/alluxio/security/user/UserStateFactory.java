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

package alluxio.security.user;

import alluxio.conf.AlluxioConfiguration;

import javax.security.auth.Subject;

/**
 * A factory for creating new UserState instances.
 */
public interface UserStateFactory {
  /**
   * @param subject the subject
   * @param conf the configuration
   * @param isServer true if this is from a server process, false otherwise
   * @return a UserState, or null if this UserState is not supported
   */
  UserState create(Subject subject, AlluxioConfiguration conf, boolean isServer);
}
