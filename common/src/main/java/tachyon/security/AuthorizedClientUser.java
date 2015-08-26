/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.security;

/**
 * An instance of this class represents a client user connecting to Tachyon service.
 *
 * It is maintained in a ThreadLocal variable based on the Thrift RPC mechanism.
 * {@link org.apache.thrift.server.TThreadPoolServer} allocates a thread to serve a connection
 * from client side and take back it when connection is closed. During the thread alive cycle,
 * all the RPC happens in this thread. These RPC methods implemented in server side could
 * get the client user by this class.
 */
public class AuthorizedClientUser {

  /**
   * A ThreadLocal variable to maintain the client user along with a specific thread.
   */
  private static ThreadLocal<User> sUserThreadLocal = new ThreadLocal<User>();

  /**
   * Create a {@link tachyon.security.User} and set it to the ThreadLocal variable.
   * @param userName the name of the client user
   */
  public static void set(String userName) {
    sUserThreadLocal.set(new User(userName));
  }

  /**
   * Get the {@link tachyon.security.User} from the ThreadLocal variable.
   * @return the client user
   */
  public static User get() {
    return sUserThreadLocal.get();
  }

  /**
   * Remove the {@link tachyon.security.User} from the ThreadLocal variable.
   */
  public static void remove() {
    sUserThreadLocal.remove();
  }
}
