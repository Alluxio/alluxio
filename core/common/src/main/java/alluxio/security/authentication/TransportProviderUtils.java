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

package alluxio.security.authentication;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.security.User;

import org.apache.thrift.transport.TSocket;

import java.net.InetSocketAddress;
import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * This class provides util methods for {@link TransportProvider}s.
 */
@ThreadSafe
public final class TransportProviderUtils {

  /**
   * @param subject the subject to use (can be null)
   * @return the configured impersonation user, or null if impersonation is not used
   */
  @Nullable
  public static String getImpersonationUser(Subject subject) {
    // The user of the hdfs client
    String hdfsUser = null;

    if (subject != null) {
      // The HDFS client uses the subject to pass in the user
      Set<User> user = subject.getPrincipals(User.class);
      if (user != null && !user.isEmpty()) {
        hdfsUser = user.iterator().next().getName();
      }
    }

    // Determine the impersonation user
    String impersonationUser = null;
    if (Configuration.containsKey(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME)) {
      impersonationUser = Configuration.get(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME);
      if (Constants.IMPERSONATION_HDFS_USER.equals(impersonationUser)) {
        // Impersonate as the hdfs client user
        impersonationUser = hdfsUser;
      }
      if (impersonationUser != null && impersonationUser.isEmpty()) {
        impersonationUser = null;
      }
    }
    return impersonationUser;
  }

  private TransportProviderUtils() {} // prevent instantiation
}
