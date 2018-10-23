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
import alluxio.PropertyKey;
import alluxio.exception.ExceptionMessage;
import alluxio.util.CommonUtils;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import javax.annotation.concurrent.ThreadSafe;
import javax.security.sasl.AuthenticationException;

/**
 * An authenticator for impersonation users. This determines if a connection user (the user
 * making the connection) is configured to impersonate as a separate impersonation user. To
 * enable impersonation for a particular connection user, the configuration parameter templates
 * {@link PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION} and/or
 * {@link PropertyKey.Template.MASTER_IMPERSONATION_GROUPS_OPTION} must be used to specify the
 * users and groups the connection user is allowed to impersonate.
 */
@ThreadSafe
public final class ImpersonationAuthenticator {
  private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
  private static final String WILDCARD = "*";
  // Maps users configured for impersonation to the set of groups which they can impersonate.
  private final Map<String, Set<String>> mImpersonationGroups;
  // Maps users configured for impersonation to the set of users which they can impersonate.
  private final Map<String, Set<String>> mImpersonationUsers;

  /**
   * Constructs a new {@link ImpersonationAuthenticator}.
   */
  public ImpersonationAuthenticator() {
    mImpersonationGroups = new HashMap<>();
    mImpersonationUsers = new HashMap<>();
    for (PropertyKey key : Configuration.keySet()) {
      String value = Configuration.getOrDefault(key, null);
      // Process impersonation groups
      Matcher matcher =
          PropertyKey.Template.MASTER_IMPERSONATION_GROUPS_OPTION.match(key.getName());
      if (matcher.matches()) {
        String connectionUser = matcher.group(1);
        if (connectionUser != null) {
          mImpersonationGroups.put(connectionUser, Sets.newHashSet(SPLITTER.split(value)));
        }
      }

      // Process impersonation users
      matcher = PropertyKey.Template.MASTER_IMPERSONATION_USERS_OPTION.match(key.getName());
      if (matcher.matches()) {
        String connectionUser = matcher.group(1);
        if (connectionUser != null) {
          mImpersonationUsers.put(connectionUser, Sets.newHashSet(SPLITTER.split(value)));
        }
      }
    }
  }

  /**
   * @param connectionUser the user of the connection
   * @param impersonationUser the user to impersonate
   * @throws AuthenticationException if the connectionUser is not allowed to impersonate the
   *         impersonationUser
   */
  public void authenticate(String connectionUser, String impersonationUser)
      throws AuthenticationException {
    if (impersonationUser == null || connectionUser.equals(impersonationUser)) {
      // Impersonation is not being used.
      return;
    }

    Set<String> allowedUsers = mImpersonationUsers.get(connectionUser);
    Set<String> allowedGroups = mImpersonationGroups.get(connectionUser);

    if (allowedUsers == null && allowedGroups == null) {
      throw new AuthenticationException(ExceptionMessage.IMPERSONATION_NOT_CONFIGURED
          .getMessage(connectionUser, impersonationUser));
    }

    // Check the impersonation users configs
    if (allowedUsers != null) {
      if (allowedUsers.contains(WILDCARD) || allowedUsers.contains(impersonationUser)) {
        // Impersonation is allowed
        return;
      }
    }

    // Check the impersonation groups configs
    if (allowedGroups != null) {
      if (allowedGroups.contains(WILDCARD)) {
        // Impersonation is allowed for all groups
        return;
      }
      try {
        for (String impersonationGroup : CommonUtils.getGroups(impersonationUser)) {
          if (allowedGroups.contains(impersonationGroup)) {
            // Impersonation is allowed for this group
            return;
          }
        }
      } catch (IOException e) {
        throw new AuthenticationException(ExceptionMessage.IMPERSONATION_GROUPS_FAILED
            .getMessage(impersonationUser, connectionUser), e);
      }
    }
    throw new AuthenticationException(
        ExceptionMessage.IMPERSONATION_DENIED.getMessage(connectionUser, impersonationUser));
  }
}
