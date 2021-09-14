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

import alluxio.annotation.SuppressFBWarnings;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;
import alluxio.security.authentication.AuthType;
import alluxio.util.CommonUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

/**
 * UserState handles logging in any client, and maintains the state. The UserState provides access
 * to the {@link Subject} and the {@link User} for the client.
 */
public interface UserState {
  /**
   * An ordered list of factories for creating UserState instances. The UserState factories are
   * tried in order, from first to last.
   */
  @SuppressFBWarnings(value = "MS_OOI_PKGPROTECT")
  ArrayList<UserStateFactory> FACTORIES = new ArrayList<>(Arrays.asList(
      new SimpleUserState.Factory(),
      new NoopUserState.Factory()
  ));

  /**
   * Returns the Subject for this user. Attempts to log in if not already logged in.
   *
   * @return the {@link Subject} for this user state
   */
  Subject getSubject();

  /**
   * Returns the User object for this user. Attempts to log in if not already logged in.
   *
   * @return the {@link User} for this user state
   * @throws UnauthenticatedException if the attempted login failed
   */
  User getUser() throws UnauthenticatedException;

  /**
   * Attempts to log in again, and returns the new User.
   *
   * @return the {@link User} for this user state, after the relogin
   * @throws UnauthenticatedException if the re-login failed
   */
  User relogin() throws UnauthenticatedException;

  /**
   * A factory for creating a UserState.
   */
  class Factory {
    private static final Logger LOG = LoggerFactory.getLogger(Factory.class);

    /**
     * @param conf the configuration to use
     * @return a new UserState instance
     */
    public static UserState create(AlluxioConfiguration conf) {
      return create(conf, new Subject());
    }

    /**
     * @param conf the configuration to use
     * @param subject the subject to use for the UserState
     * @return a new UserState instance
     */
    public static UserState create(AlluxioConfiguration conf, Subject subject) {
      return create(conf, subject, CommonUtils.PROCESS_TYPE.get());
    }

    /**
     * @param conf the configuration to use
     * @param subject the subject to use for the UserState
     * @param processType the process type to create the UserState for
     * @return a new UserState instance
     */
    public static UserState create(AlluxioConfiguration conf, Subject subject,
        CommonUtils.ProcessType processType) {
      AuthType authType = conf.getEnum(PropertyKey.SECURITY_AUTHENTICATION_TYPE, AuthType.class);
      // TODO(gpang): should this be more specific than a boolean, like PROCESS_TYPE?
      boolean isServer = !processType.equals(CommonUtils.ProcessType.CLIENT);

      for (UserStateFactory factory : FACTORIES) {
        UserState subjectState = factory.create(subject, conf, isServer);
        if (subjectState != null) {
          return subjectState;
        }
      }
      throw new UnsupportedOperationException(
          "No factory could create a UserState with authType: " + authType.getAuthName()
              + ". factories: " + String.join(", ",
              FACTORIES.stream().map((factory) -> factory.getClass().getName())
                  .collect(Collectors.toList())));
    }
  }
}
