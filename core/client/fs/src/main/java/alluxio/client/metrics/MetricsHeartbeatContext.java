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

package alluxio.client.metrics;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.master.MasterInquireClient;
import alluxio.util.IdUtils;
import alluxio.util.ThreadFactoryUtils;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A class used to track metrics heartbeats to a master.
 *
 * The class should be instantiated when a new FileSystemContext is created with a
 * configuration that points to a given master. As new FileSystemContexts are created, if they
 * utilize the same connection details, then they can simply be added to this context so
 * that their information is included in the metrics heartbeat. To add them, one should simply
 * call {@link #addHeartbeat(ClientContext, MasterInquireClient)} with the necessary arguments.
 *
 * For each separate set of connection details, a new instance of this class is created. As
 * FileSystemContexts are closed, they remove themselves from the internal metrics heartbeat.
 * When a context reaches 0 tracked contexts it will automatically close and remove itself from
 * the internal MASTER_METRICS_HEARTBEAT map.
 *
 * When the final FileSystemContext closes and removes its heartbeat from metrics it will also
 * shutdown and close the executor service until a new {@link alluxio.client.file.FileSystemContext}
 * is created.
 */
@ThreadSafe
public class MetricsHeartbeatContext {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsHeartbeatContext.class);

  /** A map from master connection details to heartbeat context instances. */
  private static final Map<MasterInquireClient.ConnectDetails, MetricsHeartbeatContext>
      MASTER_METRICS_HEARTBEAT = new ConcurrentHashMap<>(2);

  /** A value that tracks whether or not we've registered the shutdown hook for the JVM. */
  private static boolean sAddedShudownHook = false;

  /** Application ID for the JVM. Initialized lazily once the first heartbeat is added. */
  private static String sAppId = null;

  /** The service which executes metrics heartbeat RPCs. */
  private static ScheduledExecutorService sExecutorService;

  private final MasterInquireClient.ConnectDetails mConnectDetails;
  private final ClientMasterSync mClientMasterSync;
  private final AlluxioConfiguration mConf;

  // This can only be a primitive if all accesses are synchronized
  private int mCtxCount;
  private ScheduledFuture<?> mMetricsMasterHeartbeatTask;

  private MetricsHeartbeatContext(ClientContext ctx, MasterInquireClient inquireClient) {
    mCtxCount = 0;
    mConnectDetails = inquireClient.getConnectDetails();
    mConf = ctx.getClusterConf();
    mClientMasterSync = new ClientMasterSync(sAppId, ctx, inquireClient);
  }

  private synchronized void addContext() {
    // increment and lazily schedule the new heartbeat task if it is the first one
    if (mCtxCount++ == 0) {
      mMetricsMasterHeartbeatTask =
          sExecutorService.scheduleWithFixedDelay(mClientMasterSync::heartbeat,
              mConf.getMs(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS),
              mConf.getMs(PropertyKey.USER_METRICS_HEARTBEAT_INTERVAL_MS),
              TimeUnit.MILLISECONDS);
    }
  }

  private synchronized void heartbeat() {
    mClientMasterSync.heartbeat();
  }

  /**
   * Remove an application from this metrics heartbeat.
   *
   * A user who calls this method should assume the reference to this context is invalid
   * afterwards. It will automatically close and remove itself from all tracking if the number
   * of open contexts for this heartbeat reaches 0. Never attempt to add another context with
   * the same reference after removing.
   */
  private synchronized void removeContext() {
    if (--mCtxCount <= 0) {
      close();
    }
  }

  /**
   * When closed, this method will remove its task from the scheduled executor.
   *
   * It will also remove itself from being tracked in the MASTER_METRICS_HEARTBEAT. It should
   * only ever be called in {@link #removeContext()} when the context count reaches 0. Afterwards,
   * this reference should be discarded.
   */
  private synchronized void close() {
    if (mMetricsMasterHeartbeatTask != null) {
      mMetricsMasterHeartbeatTask.cancel(false);
    }
    MASTER_METRICS_HEARTBEAT.remove(mConnectDetails);
    // Trigger the last heartbeat to preserve the client side metrics changes
    heartbeat();
    mClientMasterSync.close();
  }

  /**
   * Sets up a new metrics heartbeat with the given client information.
   *
   * This will instantiate a new executor service if it is the first heartbeat to be added,
   * otherwise the application Id is simply included in an already existing heartbeat. This helps
   * to consolidate RPCs and utilize less resources on the client.
   *
   * @param ctx The application's client context
   * @param inquireClient the master inquire client used to connect to the master
   */
  public static synchronized void addHeartbeat(ClientContext ctx,
      MasterInquireClient inquireClient) {
    Preconditions.checkNotNull(ctx);
    Preconditions.checkNotNull(inquireClient);

    // Lazily initializing the executor service for first heartbeat
    // Relies on the method being synchronized
    if (sExecutorService == null) {
      sExecutorService = Executors.newSingleThreadScheduledExecutor(
          ThreadFactoryUtils.build("metrics-master-heartbeat-%d", true));
    }

    // register the shutdown hook if it hasn't been set up already
    if (!sAddedShudownHook) {
      try {
        Runtime.getRuntime().addShutdownHook(new MetricsMasterSyncShutDownHook());
        sAddedShudownHook = true;
      } catch (IllegalStateException e) {
        // this exception is thrown when the system is already in the process of shutting down. In
        // such a situation, we are about to shutdown anyway and there is no need to register this
        // shutdown hook
      } catch (SecurityException e) {
        LOG.info("Not registering metrics shutdown hook due to security exception. Regular "
            + "heartbeats will still be performed to collect metrics data, but no final "
            + "heartbeat will be performed on JVM exit. Security exception: {}", e.toString());
      }
    }

    if (sAppId == null) {
      sAppId = IdUtils.createOrGetAppIdFromConfig(ctx.getClusterConf());
      LOG.info("Created metrics heartbeat with ID {}. This ID will be used for identifying info "
              + "from the client. It can be set manually through the {} property",
          sAppId, PropertyKey.Name.USER_APP_ID);
    }

    MetricsHeartbeatContext heartbeatCtx = MASTER_METRICS_HEARTBEAT.computeIfAbsent(
        inquireClient.getConnectDetails(),
        (addr) -> new MetricsHeartbeatContext(ctx, inquireClient));
    heartbeatCtx.addContext();
    LOG.debug("Registered metrics heartbeat with appId: {}", sAppId);
  }

  /**
   * Removes an application from the metrics heartbeat.
   *
   * If this is the last application to be removed for a given master then it will cancel the
   * execution of the metrics RPC for that master.
   *
   * If this is the last application to be removed for the whole JVM, it will shutdown the
   * executor threadpool until another application needs to heartbeat. If the appId which is to
   * be removed isn't found, the application will silently continue.
   *
   * @param ctx The client context used to register the heartbeat
   */
  public static synchronized void removeHeartbeat(ClientContext ctx) {
    MasterInquireClient.ConnectDetails connectDetails =
        MasterInquireClient.Factory.getConnectDetails(ctx.getClusterConf());
    MetricsHeartbeatContext heartbeatCtx = MASTER_METRICS_HEARTBEAT.get(connectDetails);
    if (heartbeatCtx != null) {
      heartbeatCtx.removeContext();
      LOG.debug("De-registered metrics heartbeat with appId: {}", sAppId);
    }

    if (MASTER_METRICS_HEARTBEAT.isEmpty()) {
      sExecutorService.shutdown();
      try {
        sExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Metrics heartbeat executor did not shut down in a timely manner: {}",
            e.toString());
      }
      sExecutorService = null;
      sAppId = null;
    }
  }

  /**
   * Class that heartbeats to the metrics master before exit. The heartbeat is performed in a second
   * thread so that we can exit early if the heartbeat is taking too long.
   */
  private static final class MetricsMasterSyncShutDownHook extends Thread {
    private final Thread mLastHeartbeatThread;

    /**
     * Creates a new metrics master shutdown hook.
     */
    public MetricsMasterSyncShutDownHook() {
      mLastHeartbeatThread = new Thread(() -> {
        if (sExecutorService != null) {
          MASTER_METRICS_HEARTBEAT.forEach((key, value) -> value.heartbeat());
        }
      });
      mLastHeartbeatThread.setDaemon(true);
    }

    @Override
    public void run() {
      mLastHeartbeatThread.start();
      try {
        // Shutdown hooks should run quickly, so we limit the wait time to 500ms. It isn't the end
        // of the world if the final heartbeat fails.
        mLastHeartbeatThread.join(500);
      } catch (InterruptedException e) {
        return;
      } finally {
        if (mLastHeartbeatThread.isAlive()) {
          LOG.warn("Failed to heartbeat to the metrics master before exit");
        }
      }
    }
  }
}
