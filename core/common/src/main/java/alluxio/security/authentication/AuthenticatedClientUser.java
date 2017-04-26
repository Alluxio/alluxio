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

import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.security.User;
import alluxio.util.SecurityUtils;

import javax.annotation.concurrent.ThreadSafe;

/**
 * An instance of this class represents a client user connecting to {@link PlainSaslServer}.
 *
 * It is maintained in a {@link ThreadLocal} variable based on the Thrift RPC mechanism.
 * {@link org.apache.thrift.server.TThreadPoolServer} allocates a thread to serve a connection
 * from client side and take back it when connection is closed. During the thread alive cycle,
 * all the RPC happens in this thread. These RPC methods implemented in server side could
 * get the client user by this class.
 */
@ThreadSafe
public final class AuthenticatedClientUser {

  /**
   * A {@link ThreadLocal} variable to maintain the client user along with a specific thread.
   */
  private static ThreadLocal<User> sUserThreadLocal = new ThreadLocal<>();

  /**
   * Creates a {@link User} and sets it to the {@link ThreadLocal} variable.
   *
   * @param userName the name of the client user
   */
  public static void set(String userName) {
    sUserThreadLocal.set(new User(userName));
  }

  /**
   * Gets the {@link User} from the {@link ThreadLocal} variable. An exception will be thrown if
   * authentication is disabled.
   *
   * @return the client user, or null if no client user is set
   */
  public static User get() {
    if (!SecurityUtils.isAuthenticationEnabled()) {
      throw new IllegalStateException(ExceptionMessage.AUTHENTICATION_IS_NOT_ENABLED.getMessage());
    }
    return sUserThreadLocal.get();
  }

  /**
   * Gets the user name from the {@link ThreadLocal} variable. An exception will be thrown if no
   * client user has been set, or authentication is disabled.
   *
   * @return the client user's name
   */
  public static String getName() {
    User user = get();
    if (user == null) {
      throw new UnauthenticatedException(ExceptionMessage.NO_CLIENT_USER.getMessage());
    }
    return user.getName();
  }

  /**
   * Removes the {@link User} from the {@link ThreadLocal} variable.
   */
  public static synchronized void remove() {
    sUserThreadLocal.remove();
  }

  private AuthenticatedClientUser() {} // prevent instantiation
}
