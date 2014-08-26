package tachyon;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import tachyon.thrift.NetAddress;

/**
 * Transform all kinds of network address representation in Tachyon to FQDN host
 */
public class FqdnHost {
  private String mHost;
  private String mIp;

  public FqdnHost(InetAddress addr) {
    init(addr);
  }

  public FqdnHost(String host) throws UnknownHostException {
    // when host is null or empty, InetAddress.getByName will return a lookback address,
    // we don't want this behavior here
    if (host == null || host.isEmpty()) {
      mHost = mIp = null;
    } else {
      init(InetAddress.getByName(host));
    }
  }

  public FqdnHost(InetSocketAddress addr) {
    init(addr.getAddress());
  }

  public FqdnHost(NetAddress addr) throws UnknownHostException {
    init(InetAddress.getByName(addr.getMHost()));
  }

  private void init(InetAddress addr) {
    mHost = addr.getCanonicalHostName();
    mIp = addr.toString();
  }

  public String getHost() {
    return mHost;
  }

  public String getIp() {
    return mIp;
  }
}
