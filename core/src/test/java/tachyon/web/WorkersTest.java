package tachyon.web;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.master.LocalTachyonCluster;

public final class WorkersTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static final int WORKER_CAPACITY_BYTES = 1000;
  private static final int USER_QUOTA_UNIT_BYTES = 100;

  private LocalTachyonCluster mLocalTachyonCluster = null;

  @Before
  public final void before() throws IOException {
    System.setProperty("tachyon.user.quota.unit.bytes", USER_QUOTA_UNIT_BYTES + "");
    // can't go lower than 5 or it hangs
    mLocalTachyonCluster = new LocalTachyonCluster(WORKER_CAPACITY_BYTES, true);
    mLocalTachyonCluster.start();
  }

  @After
  public final void after() throws Exception {
    mLocalTachyonCluster.stop();
    System.clearProperty("tachyon.user.quota.unit.bytes");
    System.clearProperty("tachyon.worker.network.type");
  }

  @Test
  public void workersPage() throws IOException {
    InetSocketAddress address = mLocalTachyonCluster.getMasterWebAddress();

    HttpClient client = new DefaultHttpClient();
    try {
      String url = String.format("http://%s:%d/workers", address.getHostName(), address.getPort());
      LOG.info("Requesting page {}", url);
      HttpGet httpget = new HttpGet(url);
      ResponseHandler<String> responseHandler = new BasicResponseHandler();
      String responseBody = client.execute(httpget, responseHandler);
      LOG.info("Workers page body:\n{}", responseBody);
    } finally {
      client.getConnectionManager().shutdown();
    }
  }
}
