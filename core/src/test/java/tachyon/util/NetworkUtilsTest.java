package tachyon.util;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.junit.Assert;
import org.junit.Test;

import tachyon.TachyonURI;
import tachyon.thrift.NetAddress;

public class NetworkUtilsTest {

  @Test
  public void replaceHostNameTest() throws UnknownHostException {
    Assert.assertEquals(NetworkUtils.replaceHostName(""), null);
    Assert.assertEquals(NetworkUtils.replaceHostName((String) null), null);

    String[] paths =
        new String[] { "hdfs://localhost:9000/dir", "hdfs://localhost/dir", "hdfs://localhost/",
            "hdfs://localhost", "file:///dir", "/dir", "anythingElse" };

    for (String path : paths) {
      Assert.assertEquals(NetworkUtils.replaceHostName(path), path);
    }
  }

  @Test
  public void replaceHostNameTest2() throws UnknownHostException {
    Assert.assertEquals(NetworkUtils.replaceHostName(TachyonURI.EMPTY_URI), TachyonURI.EMPTY_URI);
    Assert.assertEquals(NetworkUtils.replaceHostName((TachyonURI) null), null);

    TachyonURI[] paths =
        new TachyonURI[] { new TachyonURI("hdfs://localhost:9000/dir"),
            new TachyonURI("hdfs://localhost/dir"), new TachyonURI("hdfs://localhost/"),
            new TachyonURI("hdfs://localhost"), new TachyonURI("file:///dir"),
            new TachyonURI("/dir"), new TachyonURI("anythingElse") };

    for (TachyonURI path : paths) {
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
