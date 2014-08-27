package tachyon;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.google.common.net.HostAndPort;

import tachyon.thrift.NetAddress;

/**
 * Transform all kinds of network address representation in Tachyon to FQDN host
 */
public class FqdnHost {
  private HostAndPort mHp;
  private String mIp;

  public FqdnHost(InetAddress addr) {
    init(addr);
  }

  public FqdnHost(String host) throws UnknownHostException {
    // when host is null or empty, InetAddress.getByName will return a lookback address,
    // we don't want this behavior here
    if (host == null || host.isEmpty()) {
      throw new UnknownHostException("host can not be null or empty");
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
    mHp = HostAndPort.fromString(addr.getCanonicalHostName());
    mIp = addr.getHostAddress();
  }

  public String getHost() {
    return mHp.getHostText();
  }

  public String getIp() {
    return mIp;
  }
}
