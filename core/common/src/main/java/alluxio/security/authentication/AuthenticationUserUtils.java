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

import alluxio.Constants;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.CurrentUser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import javax.security.auth.Subject;

/**
 * This class provides util methods for {@link AuthenticationUserUtils}s.
 */
@ThreadSafe
public final class AuthenticationUserUtils {
  private static final Logger LOG = LoggerFactory.getLogger(AuthenticationUserUtils.class);

  /**
   * @param subject the subject to use (can be null)
   * @param conf Alluxio configuration
   * @return the configured impersonation user, or null if impersonation is not used
   */
  @Nullable
  public static String getImpersonationUser(Subject subject, AlluxioConfiguration conf) {
    // The user of the hdfs client
    String hdfsUser = null;

    if (subject != null) {
      // The HDFS client uses the subject to pass in the user
      Set<CurrentUser> user = subject.getPrincipals(CurrentUser.class);
      LOG.debug("Impersonation: subject: {}", subject);
      if (user != null && !user.isEmpty()) {
        hdfsUser = user.iterator().next().getName();
      }
    }

    // Determine the impersonation user
    String impersonationUser = null;
    if (conf.isSet(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME)) {
      impersonationUser = conf.get(PropertyKey.SECURITY_LOGIN_IMPERSONATION_USERNAME);
      LOG.debug("Impersonation: configured: {}", impersonationUser);
      if (Constants.IMPERSONATION_HDFS_USER.equals(impersonationUser)) {
        // Impersonate as the hdfs client user
        impersonationUser = hdfsUser;
      } else {
        // do not use impersonation, for any value that is not _HDFS_USER_
        if (impersonationUser != null && !impersonationUser.isEmpty()
            && !Constants.IMPERSONATION_NONE.equals(impersonationUser)) {
          LOG.warn("Impersonation ignored. Invalid configuration: {}", impersonationUser);
        }
        impersonationUser = null;
      }
      if (impersonationUser != null && impersonationUser.isEmpty()) {
        impersonationUser = null;
      }
    }
    LOG.debug("Impersonation: hdfsUser: {} impersonationUser: {}", hdfsUser, impersonationUser);
    return impersonationUser;
  }

  private AuthenticationUserUtils() {} // prevent instantiation
}
