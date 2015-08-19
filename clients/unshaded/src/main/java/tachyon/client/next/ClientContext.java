package tachyon.client.next;

import java.net.InetSocketAddress;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * A shared context in each client JVM. It provides common functionality such as the Tachyon
 * configuration and master address. All members of this class are immutable. This class is
 * thread safe.
 */
public class ClientContext {
  /**
   * The static configuration object. There is only one TachyonConf object shared within the same
   * client.
   */
  private static final TachyonConf TACHYON_CONF;

  private static final InetSocketAddress MASTER_ADDRESS;

  static {
    TACHYON_CONF = new TachyonConf();

    String masterHostname = Preconditions.checkNotNull(TACHYON_CONF.get(Constants.MASTER_HOSTNAME));
    int masterPort = TACHYON_CONF.getInt(Constants.MASTER_PORT);

    MASTER_ADDRESS = new InetSocketAddress(masterHostname, masterPort);
  }

  /**
   * Returns the one and only static {@link TachyonConf} object which is shared among all classes
   * within the client
   *
   * @return the tachyonConf for the worker process
   */
  public static TachyonConf getConf() {
    return TACHYON_CONF;
  }

  public static InetSocketAddress getMasterAddress() {
    return MASTER_ADDRESS;
  }
}
