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

package alluxio.master.callhome;

import alluxio.CallHomeConstants;
import alluxio.Constants;
import alluxio.ProjectConstants;
import alluxio.Server;
import alluxio.clock.SystemClock;
import alluxio.heartbeat.HeartbeatContext;
import alluxio.heartbeat.HeartbeatExecutor;
import alluxio.heartbeat.HeartbeatThread;
import alluxio.master.AbstractNonJournaledMaster;
import alluxio.master.MasterContext;
import alluxio.master.MasterProcess;
import alluxio.master.MasterRegistry;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.util.executor.ExecutorServiceFactories;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.NetworkInterface;
import java.net.URL;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This service periodically collects call home information.
 */
@ThreadSafe
public final class CallHomeMaster extends AbstractNonJournaledMaster {
  private static final Logger LOG = LoggerFactory.getLogger(CallHomeMaster.class);
  private static final Set<Class<? extends Server>> DEPS =
      ImmutableSet.of(BlockMaster.class, FileSystemMaster.class);
  private static final String TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ssXXX"; // RFC3339

  /** The Alluxio master process. */
  private MasterProcess mMasterProcess;

  /**
   * The service that performs license check.
   */
  @SuppressFBWarnings("URF_UNREAD_FIELD")
  private Future<?> mCallHomeService;

  /**
   * Creates a new instance of {@link CallHomeMaster}.
   *
   * @param registry the master registry
   * @param masterContext the context for Alluxio master
   */
  public CallHomeMaster(MasterRegistry registry, MasterContext masterContext) {
    super(masterContext, new SystemClock(), ExecutorServiceFactories
        .fixedThreadPoolExecutorServiceFactory(Constants.CALL_HOME_MASTER_NAME, 2));
    registry.add(CallHomeMaster.class, this);
  }

  /**
   * Sets the master to be used in {@link CallHomeExecutor} for collecting call home information.
   * This should be called before calling {@link #start(Boolean)}.
   *
   * @param masterProcess the Alluxio master process
   */
  public void setMaster(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  @Override
  public Set<Class<? extends Server>> getDependencies() {
    return DEPS;
  }

  @Override
  public String getName() {
    return Constants.CALL_HOME_MASTER_NAME;
  }

  @Override
  public void start(Boolean isLeader) throws IOException {
    super.start(isLeader);
    Preconditions.checkNotNull(mMasterProcess, "Alluxio master process is not specified");
    if (!isLeader) {
      return;
    }
    LOG.info("Starting {}", getName());
    mCallHomeService = getExecutorService().submit(
        new HeartbeatThread(HeartbeatContext.MASTER_CALL_HOME, new CallHomeExecutor(mMasterProcess),
            Long.parseLong(CallHomeConstants.CALL_HOME_PERIOD_MS)));
    LOG.info("{} is started", getName());
  }

  @Override
  public Map<String, TProcessor> getServices() {
    return new HashMap<>();
  }

  /**
   * Collects and saves call home information during the heartbeat.
   */
  @ThreadSafe
  public static final class CallHomeExecutor implements HeartbeatExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(CallHomeExecutor.class);

    private MasterProcess mMasterProcess;
    private BlockMaster mBlockMaster;
    private FileSystemMaster mFsMaster;

    /**
     * Creates a new instance of {@link CallHomeExecutor}.
     *
     * @param masterProcess the Alluxio master process
     */
    public CallHomeExecutor(MasterProcess masterProcess) {
      mMasterProcess = masterProcess;
      mBlockMaster = masterProcess.getMaster(BlockMaster.class);
      mFsMaster = masterProcess.getMaster(FileSystemMaster.class);
    }

    @Override
    public void heartbeat() throws InterruptedException {
      if (!mMasterProcess.isServing()) {
        // Collect call home information only when master is up and running.
        return;
      }
      CallHomeInfo info = null;
      try {
        info = collect();
      } catch (IOException e) {
        LOG.error("Failed to collect call home information: {}", e);
      }
      if (info == null) {
        return;
      }
      try {
        upload(info);
      } catch (IOException e) {
        // When clusters do not have internet connection, this error is expected, so the error is
        // only logged as warnings.
        LOG.warn("Failed to upload call home information: {}", e.getMessage());
      }
    }

    @Override
    public void close() {
      // Nothing to close.
    }

    /**
     * @return the collected call home information, null if license hasn't been loaded
     * @throws IOException when failed to collect call home information
     */
    private CallHomeInfo collect() throws IOException {
      return CallHomeUtils.collectDiagnostics(mMasterProcess, mBlockMaster, mFsMaster);
    }

    /**
     * @return the MAC address of the network interface being used by the master
     * @throws IOException when no MAC address is found
     */
    private byte[] getMACAddress() throws IOException {
      // Try to get the MAC address of the network interface of the master's RPC address.
      NetworkInterface nic =
          NetworkInterface.getByInetAddress(mMasterProcess.getRpcAddress().getAddress());
      byte[] mac = nic.getHardwareAddress();
      if (mac != null) {
        return mac;
      }

      // Try to get the MAC address of the common "en0" interface.
      nic = NetworkInterface.getByName("en0");
      mac = nic.getHardwareAddress();
      if (mac != null) {
        return mac;
      }

      // Try to get the first non-empty MAC address in the enumeration of all network interfaces.
      Enumeration<NetworkInterface> ifaces = NetworkInterface.getNetworkInterfaces();
      while (ifaces.hasMoreElements()) {
        nic = ifaces.nextElement();
        mac = nic.getHardwareAddress();
        if (mac != null) {
          return mac;
        }
      }

      throw new IOException("No MAC address was found");
    }

    /**
     * @return the object key of the call home information
     * @throws IOException when failed to construct the object key
     */
    private String getObjectKey() throws IOException {
      // Get time related information.
      Date now = new Date(System.currentTimeMillis());
      DateFormat formatter = new SimpleDateFormat(TIME_FORMAT);
      String time = formatter.format(now);
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(now);
      int year = calendar.get(Calendar.YEAR);
      int month = calendar.get(Calendar.MONTH) + 1; // get(Calendar.MONTH) returns 0 for January
      int day = calendar.get(Calendar.DAY_OF_MONTH);

      // Get MAC address.
      StringBuilder sb = new StringBuilder(18);
      for (byte b : getMACAddress()) {
        if (sb.length() > 0) {
          sb.append(':');
        }
        sb.append(String.format("%02x", b));
      }
      String mac = sb.toString();

      Joiner joiner = Joiner.on("/");
      return joiner.join("user", year, month, day, mac + "-" + time);
    }

    /**
     * Uploads the collected call home information to Alluxio company's backend.
     *
     * @param info the call home information to be uploaded
     */
    private void upload(CallHomeInfo info) throws IOException {
      // Encode info into json as payload.
      String payload = "";
      try {
        ObjectMapper mapper = new ObjectMapper();
        payload = mapper.writeValueAsString(info);
      } catch (JsonProcessingException e) {
        throw new IOException("Failed to encode CallHomeInfo as json: " + e);
      }

      // Create the upload request.
      String objectKey = getObjectKey();
      Joiner joiner = Joiner.on("/");
      String path = joiner.join("upload", CallHomeConstants.CALL_HOME_BUCKET, objectKey);
      String url = new URL(new URL(ProjectConstants.PROXY_URL), path).toString();
      HttpPost post = new HttpPost(url);
      MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      builder.addBinaryBody("payload", payload.getBytes(), ContentType.APPLICATION_OCTET_STREAM,
          "payload");
      post.setEntity(builder.build());

      // Fire off the upload request.
      HttpClient client = HttpClientBuilder.create().build();
      HttpResponse response = client.execute(post);

      // Check the response code.
      int responseCode = response.getStatusLine().getStatusCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("Call home upload request failed with: " + responseCode);
      }
    }
  }
}
