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

package tachyon.network;

import javax.annotation.concurrent.ThreadSafe;

import io.netty.channel.epoll.Epoll;

/**
 * What type of netty channel to use. {@link #NIO} and {@link #EPOLL} are supported currently.
 */
@ThreadSafe
public enum ChannelType {
  NIO,
  /**
   * Use Linux's epoll for channel API. This type of channel only works on Linux.
   */
  EPOLL;

  /**
   * Determines the default type to use based off the system.
   * <p>
   * On Linux-based systems, {@link #EPOLL} is the default type for more consistent performance,
   * otherwise {@link #NIO}.
   * </p>
   *
   * @return a {@link ChannelType} compatible with the host
   */
  public static ChannelType defaultType() {
    if (Epoll.isAvailable()) {
      return ChannelType.EPOLL;
    } else {
      return ChannelType.NIO;
    }
  }
}
