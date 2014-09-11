package tachyon.worker.netty;

import io.netty.channel.epoll.Epoll;

/**
 * What type of netty channel to use.
 */
public enum ChannelType {
  NIO,
  /**
   * Use Linux's epoll for channel api. Only works on linux
   */
  EPOLL;

  /**
   * Determines the default type to use based off the system.
   * <p />
   * On linux based systems, epoll will be selected for more consistent performance, for everything
   * else nio is returned.
   */
  public static ChannelType defaultType() {
    if (Epoll.isAvailable()) {
      return ChannelType.EPOLL;
    } else {
      return ChannelType.NIO;
    }
  }
}
