package tachyon.util;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.thrift.NetAddress;

public class NetworkUtilsTest {

  @Test
  public void replaceHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkUtils.replaceHostName(""), null);
    Assert.assertEquals(NetworkUtils.replaceHostName(null), null);

    String[] paths =
        new String[] {"hdfs://localhost:9000/dir", "hdfs://localhost/dir", "hdfs://localhost/",
            "hdfs://localhost", "file:///dir", "/dir", "anythingElse"};

    for (String path : paths) {
      Assert.assertEquals(NetworkUtils.replaceHostName(path), path);
    }
  }

  @Test
  public void resolveHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkUtils.resolveHostName(""), null);
    Assert.assertEquals(NetworkUtils.resolveHostName(null), null);
    Assert.assertEquals(NetworkUtils.resolveHostName("localhost"), "localhost");
  }

  @Test
  public void getFqdnHostTest() throws UnknownHostException {
    Assert.assertEquals(NetworkUtils.getFqdnHost(new InetSocketAddress("localhost", 0)),
        "localhost");
    Assert.assertEquals(NetworkUtils.getFqdnHost(new NetAddress("localhost", 0, 0)), "localhost");
  }
}
