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
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import javax.annotation.concurrent.ThreadSafe;

/**
 * This service periodically collects call home information and stores it to Alluxio company's
 * backend.
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
      } catch (IOException | GeneralSecurityException e) {
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
    private CallHomeInfo collect() throws IOException, GeneralSecurityException {
      return CallHomeUtils.collectDiagnostics(mMasterProcess, mBlockMaster, mFsMaster);
    }

    /**
     * Uploads the collected call home information to Alluxio company's backend.
     *
     * @param info the call home information to be uploaded
     */
    private void upload(CallHomeInfo info) throws IOException {
      String body = "";
      try {
        ObjectMapper mapper = new ObjectMapper();
        body = mapper.writeValueAsString(info);
      } catch (JsonProcessingException e) {
        throw new IOException("Failed to encode CallHomeInfo as json: " + e);
      }

      // Create the upload request.
      Joiner joiner = Joiner.on("/");
      String path = joiner.join("v0", "upload");
      String url = new URL(new URL(CallHomeConstants.CALL_HOME_HOST), path).toString();

      HttpPost post = new HttpPost(url);
      post.setEntity(new StringEntity(body));
      post.setHeader("Content-type", "application/json");

      // Fire off the upload request.
      HttpClient client = HttpClientBuilder.create().build();
      HttpResponse response = client.execute(post);

      // Check the response code.
      int responseCode = response.getStatusLine().getStatusCode();
      if (responseCode != HttpURLConnection.HTTP_OK) {
        throw new IOException("Call home upload request failed with code: " + responseCode);
      }
    }
  }
}
