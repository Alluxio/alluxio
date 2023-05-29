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

package alluxio.cli.fsadmin.command;

import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.metrics.MetricKey;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeoutRetry;
import alluxio.util.FormatUtils;
import alluxio.util.SleepUtils;
import alluxio.util.network.HttpUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.wire.WorkerWebUIOperations;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Decommission a specific batch of workers, the decommissioned worker is not automatically
 * shutdown and will not be picked for new I/O requests. The workers still serve concurrent
 * requests and eventually will become idle. This command waits for the workers to be idle.
 * So when this command returns, it will be safe for the admin to kill/restart those workers.
 * See the help message for more details.
 */
public final class DecommissionWorkerCommand extends AbstractFsAdminCommand {
  private static final Logger LOG = LoggerFactory.getLogger(DecommissionWorkerCommand.class);
  private static final int DEFAULT_WAIT_TIME_MS = 5 * Constants.MINUTE_MS; // 5min

  private static final Option ADDRESSES_OPTION =
      Option.builder("a")
          .longOpt("addresses")
          .required(true)  // Host option is mandatory.
          .hasArg(true)
          .numberOfArgs(1)
          .argName("workerHosts")
          .desc("One or more worker addresses separated by comma. If port is not specified, "
              + PropertyKey.WORKER_WEB_PORT.getName() + " will be used. The command will talk "
              + "to the workers' web port to monitor if they are idle and safe to stop.")
          .build();
  private static final Option WAIT_OPTION =
      Option.builder("w")
          .longOpt("wait")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("waitTime")
          .desc("Time to wait, in human readable form like 5m.")
          .build();
  private static final Option DISABLE_OPTION =
      Option.builder("d")
          .longOpt("disable")
          .required(false)
          .hasArg(false)
          .desc("Whether the worker should be disabled and not allowed to register again, "
              + "until it is re-enabled again by fsadmin enableWorker command.")
          .build();

  private final Set<WorkerNetAddress> mFailedWorkers = new HashSet<>();
  private final Map<WorkerNetAddress, WorkerStatus> mWaitingWorkers = new HashMap<>();
  private final Set<WorkerNetAddress> mFinishedWorkers = new HashSet<>();
  private final Set<WorkerNetAddress> mLostWorkers = new HashSet<>();
  private final AlluxioConfiguration mConf;

  /**
   * Constructs a new instance to decommission a given batch of workers from Alluxio.
   *
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public DecommissionWorkerCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  private BlockWorkerInfo findMatchingWorkerAddress(
      WorkerNetAddress address, List<BlockWorkerInfo> cachedWorkers) {
    for (BlockWorkerInfo worker : cachedWorkers) {
      if (worker.getNetAddress().getHost().equals(address.getHost())) {
        return worker;
      }
    }
    throw new IllegalArgumentException("Worker " + address.getHost()
        + " is not known by the master. Please check the hostname or retry later. "
        + "Available workers are: " + printCachedWorkerAddresses(cachedWorkers));
  }

  private String printCachedWorkerAddresses(List<BlockWorkerInfo> cachedWorkers) {
    StringBuilder sb = new StringBuilder();
    for (BlockWorkerInfo blockWorkerInfo : cachedWorkers) {
      sb.append("\t").append(blockWorkerInfo.getNetAddress().getHost()).append(":")
          .append(blockWorkerInfo.getNetAddress().getWebPort());
    }
    return sb.toString();
  }

  private long parseWaitTimeMs(CommandLine cl) {
    if (cl.hasOption(WAIT_OPTION.getLongOpt())) {
      String waitTimeStr = cl.getOptionValue(WAIT_OPTION.getLongOpt());
      return FormatUtils.parseTimeSize(waitTimeStr);
    } else {
      return DEFAULT_WAIT_TIME_MS;
    }
  }

  @Override
  public int run(CommandLine cl) {
    long waitTimeMs = parseWaitTimeMs(cl);
    FileSystemContext context = FileSystemContext.create();
    List<BlockWorkerInfo> availableWorkers;
    try {
      availableWorkers = context.getCachedWorkers();
    } catch (Exception e) {
      System.err.format("Cannot get available worker list from master: %s%n", e.getMessage());
      LOG.error("Failed to get worker list from master", e);
      return ReturnCode.LOST_MASTER_CONNECTION.getCode();
    }

    // The decommission command is idempotent
    sendDecommissionCommand(cl, availableWorkers);
    System.out.format("Sent decommission messages to the master, %s failed and %s succeeded%n",
        mFailedWorkers.size(), mWaitingWorkers.size());
    System.out.format("Failed ones: %s%n", mFailedWorkers.stream()
        .map(WorkerAddressUtils::convertAddressToStringWebPort).collect(Collectors.toList()));
    if (mWaitingWorkers.size() == 0) {
      System.out.println(ReturnCode.DECOMMISSION_FAILED.getMessage());
      return ReturnCode.DECOMMISSION_FAILED.getCode();
    }

    // Manually block and wait, for all clients(proxies) to see the update on the worker list
    verifyFromMasterAndWait(context, mWaitingWorkers.keySet());

    // Block and wait for the workers to become idle, so when this command returns without error,
    // the admin is safe to proceed to stopping those workers
    Instant startWaiting = Instant.now();
    waitForWorkerToBecomeIdle(startWaiting, waitTimeMs);
    Instant end = Instant.now();
    System.out.format("Waited %s minutes for workers to be idle%n",
        Duration.between(startWaiting, end).toMinutes());

    if (mWaitingWorkers.size() > 0 || mLostWorkers.size() > 0) {
      if (mWaitingWorkers.size() > 0) {
        System.out.format("%s workers still have not finished all their operations%n",
            mWaitingWorkers.keySet());
        System.out.println("The admin should manually intervene and check those workers, "
            + "before shutting them down.");
        for (Map.Entry<WorkerNetAddress, WorkerStatus> entry : mWaitingWorkers.entrySet()) {
          WorkerWebUIOperations lastSeenStatus = entry.getValue().getWorkerTrackedStatus();
          System.out.format("Worker %s has %s=%s, %s=%s%n",
              WorkerAddressUtils.convertAddressToStringWebPort(entry.getKey()),
              MetricKey.WORKER_ACTIVE_OPERATIONS.getName(),
              lastSeenStatus.getOperationCount(),
              MetricKey.WORKER_RPC_QUEUE_LENGTH.getName(),
              lastSeenStatus.getRpcQueueLength());
        }
      }
      if (mLostWorkers.size() > 0) {
        System.out.format("%s workers finished all their operations successfully:%n%s%n",
            mFinishedWorkers.size(),
            WorkerAddressUtils.workerAddressListToString(mFinishedWorkers));
        System.out.format("%s workers became inaccessible and we assume there are no operations, "
            + "but we still recommend the admin to double check:%n%s%n",
            mLostWorkers.size(),
            WorkerAddressUtils.workerAddressListToString(mLostWorkers));
      }
      return mWaitingWorkers.size() > 0 ? ReturnCode.WORKERS_NOT_IDLE.getCode()
              : ReturnCode.LOST_SOME_WORKERS.getCode();
    } else {
      System.out.println(ReturnCode.OK.getMessage());
      return ReturnCode.OK.getCode();
    }
  }

  private void sendDecommissionCommand(CommandLine cl, List<BlockWorkerInfo> availableWorkers) {
    boolean canRegisterAgain = !cl.hasOption(DISABLE_OPTION.getLongOpt());
    String workerAddressesStr = cl.getOptionValue(ADDRESSES_OPTION.getLongOpt());
    if (workerAddressesStr.isEmpty()) {
      throw new IllegalArgumentException("Worker addresses must be specified");
    }
    List<WorkerNetAddress> addresses =
        WorkerAddressUtils.parseWorkerAddresses(workerAddressesStr, mConf);
    for (WorkerNetAddress a : addresses) {
      System.out.format("Decommissioning worker %s%n",
          WorkerAddressUtils.convertAddressToStringWebPort(a));

      BlockWorkerInfo worker = findMatchingWorkerAddress(a, availableWorkers);
      WorkerNetAddress workerAddress = worker.getNetAddress();
      DecommissionWorkerPOptions options =
          DecommissionWorkerPOptions.newBuilder()
              .setWorkerHostname(workerAddress.getHost())
              .setWorkerWebPort(workerAddress.getWebPort())
              .setCanRegisterAgain(canRegisterAgain).build();
      try {
        mBlockClient.decommissionWorker(options);
        System.out.format("Set worker %s decommissioned on master%n",
            WorkerAddressUtils.convertAddressToStringWebPort(workerAddress));
        // Start counting for this worker
        mWaitingWorkers.put(worker.getNetAddress(), new WorkerStatus());
      } catch (IOException ie) {
        System.err.format("Failed to decommission worker %s%n",
            WorkerAddressUtils.convertAddressToStringWebPort(workerAddress));
        ie.printStackTrace();
        mFailedWorkers.add(workerAddress);
      }
    }
  }

  private void waitForWorkerToBecomeIdle(Instant startWaiting, long waitTimeMs) {
    // Block and wait for the workers to become idle, so when this command returns without error,
    // the admin is safe to proceed to stopping those workers
    boolean helpPrinted = false;
    // Sleep 1s until the target time
    RetryPolicy retry = new TimeoutRetry(startWaiting.toEpochMilli() + waitTimeMs, 1000);
    while (mWaitingWorkers.size() > 0 && retry.attempt()) {
      // Poll the status from each worker
      for (Map.Entry<WorkerNetAddress, WorkerStatus> entry : mWaitingWorkers.entrySet()) {
        WorkerNetAddress address = entry.getKey();
        System.out.format("Polling status from worker %s%n",
            WorkerAddressUtils.convertAddressToStringWebPort(address));
        try {
          WorkerWebUIOperations workerStatus = pollWorkerStatus(address);
          entry.getValue().recordWorkerStatus(workerStatus);
          if (canWorkerBeStopped(workerStatus)) {
            entry.getValue().countWorkerIsQuiet();
          } else {
            /*
             * If there are operations on the worker, clear the counter.
             * The worker is considered idle only if there are zero operations in
             * consecutive checks.
             */
            entry.getValue().countWorkerNotQuiet();
          }
        } catch (Exception e) {
          System.err.format("Failed to poll progress from worker %s: %s%n",
              address.getHost(), e.getMessage());
          if (!helpPrinted) {
            printWorkerNoResponseReasons();
            helpPrinted = true;
          }
          LOG.error("Failed to poll progress from worker", e);
          entry.getValue().countError();
        }
      }

      mWaitingWorkers.entrySet().removeIf(entry -> {
        boolean isQuiet = entry.getValue().isWorkerQuiet();
        if (isQuiet) {
          System.out.format("There is no operation on worker %s:%s for %s times in a row. "
              + "Worker is considered safe to stop.%n", entry.getKey().getHost(),
              entry.getKey().getWebPort(), WorkerStatus.WORKER_QUIET_THRESHOLD);
          mFinishedWorkers.add(entry.getKey());
          return true;
        }
        boolean isError = entry.getValue().isWorkerInaccessible();
        if (isError) {
          System.out.format("Failed to poll status from worker %s:%s for %s times in a row. "
              + "Worker is considered inaccessible and not functioning. "
              + "If the worker is not functioning, it probably does not currently have ongoing "
              + "I/O and can be stopped. But the admin is advised to manually double check the "
              + "working before stopping it. %n",
              entry.getKey().getHost(), entry.getKey().getWebPort(),
                  WorkerStatus.WORKER_ERROR_THRESHOLD);
          mLostWorkers.add(entry.getKey());
          return true;
        }
        return false;
      });
    }
  }

  private static void printWorkerNoResponseReasons() {
    System.err.println("There are many reasons why the poll can fail, including but not limited "
        + "to:");
    System.err.println("1. Worker is running with a low version which does not contain "
        + "this endpoint");
    System.err.println("2. alluxio.worker.web.port is not configured correctly or is not "
        + "accessible by firewall rules");
    System.err.println("3. Some other transient network errors");
  }

  // We verify the target workers have been taken off the list on the master
  // Then we manually block for a while so clients/proxies in the cluster all get the update
  private void verifyFromMasterAndWait(
      FileSystemContext context, Collection<WorkerNetAddress> removedWorkers) {
    // Wait a while so the proxy instances will get updated worker list from master
    long workerListLag = mConf.getMs(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL);
    System.out.format("Clients take %s=%s to be updated on the new worker list so this command "
            + "will block for the same amount of time to ensure the update propagates to clients "
            + "in the cluster.%n",
        PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL.getName(),
        mConf.get(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL));
    SleepUtils.sleepMs(workerListLag);

    // Poll the latest worker list and verify the workers are decommissioned
    System.out.println("Verifying the decommission has taken effect by listing all "
        + "available workers on the master");
    try {
      Set<BlockWorkerInfo> cachedWorkers = new HashSet<>(context.getCachedWorkers());
      System.out.println("Now on master the available workers are: "
          + WorkerAddressUtils.workerListToString(cachedWorkers));
      cachedWorkers.forEach(w -> {
        if (removedWorkers.contains(w.getNetAddress())) {
          System.err.format("Worker %s is still showing available on the master. "
              + "Please check why the decommission did not work.%n", w.getNetAddress());
          System.err.println("This command will still continue, but the admin should manually "
              + "verify the state of this worker afterwards.");
        }
      });
    } catch (IOException e) {
      System.err.format("Failed to refresh the available worker list from master: %s%n",
          e.getMessage());
      System.err.println("The command will skip this check and continue. If we observe "
          + "the workers become idle, that suggests the decommission is successful and no clients "
          + "will be using this batch of workers, and this error can be ignored.");
      LOG.error("Failed to refresh the available worker list from master", e);
    }
  }

  @VisibleForTesting
  private static WorkerWebUIOperations pollWorkerStatus(WorkerNetAddress worker)
      throws IOException {
    URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme("http");
    uriBuilder.setHost(worker.getHost());
    uriBuilder.setPort(worker.getWebPort());
    uriBuilder.setPath(Constants.REST_API_PREFIX + "/worker/operations");

    // Poll the worker status endpoint
    AtomicReference<WorkerWebUIOperations> workerState = new AtomicReference<>();
    HttpUtils.get(uriBuilder.toString(), 5000, inputStream -> {
      ObjectMapper mapper = new ObjectMapper();
      workerState.set(mapper.readValue(inputStream, WorkerWebUIOperations.class));
    });

    if (workerState.get() == null) {
      // Should not reach here
      throw new IOException("Received null from worker operation status!");
    }
    return workerState.get();
  }

  private static boolean canWorkerBeStopped(WorkerWebUIOperations workerStatus) {
    // Now the idleness check only considers RPCs. This means it does NOT consider
    // short circuit r/w operations. So when the admin believes the worker is idle and
    // kill/restart the worker, ongoing r/w operations may fail.
    // https://github.com/Alluxio/alluxio/issues/17343
    /*
     * The operation count consists of ongoing operations in worker thread pools:
     * 1. RPC pool
     * 2. Data reader pool (used for reading block contents)
     * 3. Data reader serialized pool (used for replying read requests)
     * 4. Data writer pool
     * So if the operation count goes to zero that means all pools are idle.
     *
     * Pool 2, 3 and 4 all have a very small queue so only the queue 1 length is helpful.
     */
    boolean result = workerStatus.getOperationCount() == 0 && workerStatus.getRpcQueueLength() == 0;
    if (!result) {
      System.out.format("Worker ActiveOperations=%s, RpcQueueLength=%s%n",
              workerStatus.getOperationCount(), workerStatus.getRpcQueueLength());
    }
    return result;
  }

  @Override
  public String getCommandName() {
    return "decommissionWorker";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ADDRESSES_OPTION)
        .addOption(WAIT_OPTION).addOption(DISABLE_OPTION);
  }

  @Override
  public String getUsage() {
    return "decommissionWorker --addresses <workerHosts> [--wait waitTime] [--disable]";
  }

  @Override
  public String getDescription() {
    return "Decommission a specific batch of workers in the Alluxio cluster. "
        + "The command will perform the following actions:\n"
        + "1. For each worker in the batch, send a decommission command to the primary Alluxio "
        + "master so the master marks those workers as decommissioned and will not serve "
        + "operations.\n"
        + "2. It takes a small interval for all other Alluxio components (like clients and "
        + "Proxy instances) to know those workers should not be used, so this command waits for "
        + "the interval time defined by " + PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL + ".\n"
        + "3. Gets the active worker list from the master after waiting, and verify the target "
        + "workers are not active anymore.\n"
        + "4. Wait for the workers to become idle. This command will constantly check the "
        + "idleness status on each worker.\n"
        + "5. Either all workers have become idle, or the specified timeout expires, this command "
        + "will return.\n"
        + "\n" // One empty line
        + "This command is idempotent and can be retried, but the admin is advised to manually "
        + "check if there's an error. The return codes have different meanings: "
        + printReturnCodes();
  }

  private String printReturnCodes() {
    StringBuilder sb = new StringBuilder();
    for (ReturnCode rc : ReturnCode.values()) {
      sb.append("\n").append(rc.getCode()).append(": ").append(rc.name());
      sb.append("\n").append(rc.getMessage());
    }
    return sb.toString();
  }

  /**
   * A set of return codes.
   * Each code embeds an exit code (like 0 or 1) and a message for the admin.
   */
  public enum ReturnCode {
    OK(0, "All workers are successfully decommissioned and now idle. Safe to kill or "
        + "restart this batch of workers now."),
    DECOMMISSION_FAILED(1, "Failed to decommission all workers. "
        + "The admin should double check the worker addresses and the primary master status."),
    LOST_MASTER_CONNECTION(2, "Lost connection to the primary master while this "
        + "command is running. This suggests the configured master address is wrong or the "
        + "primary master failed over."),
    // Some workers are still not idle so they are not safe to restart.
    WORKERS_NOT_IDLE(3, "Some workers were still not idle afte the wait. "
        + "Either the wait time is too short or those workers failed to mark decommissioned. "
        + "The admin should manually intervene and check those workers."),
    LOST_SOME_WORKERS(10, "Workers are decommissioned but some or all workers "
        + "lost contact while this command is running. If a worker is not serving then it is "
        + "safe to kill or restart. But the admin is advised to double check the status of "
        + "those workers.")
    ;

    private final int mCode;
    private final String mMessage;

    /**
     * Constructor.
     *
     * @param code the code to exit with
     * @param message the message to display
     */
    ReturnCode(int code, String message) {
      mCode = code;
      mMessage = message;
    }

    /**
     * Gets the code.
     * @return the code
     */
    public int getCode() {
      return mCode;
    }

    /**
     * Gets the message.
     * @return the message
     */
    public String getMessage() {
      return mMessage;
    }
  }

  /**
   * A wrapper managing worker activeness status and deciding whether the worker
   * can be safely killed.
   */
  public static class WorkerStatus {
    public static final int WORKER_QUIET_THRESHOLD = 20;
    public static final int WORKER_ERROR_THRESHOLD = 5;

    private final LongAdder mConsecutiveQuietCount;
    private final LongAdder mConsecutiveFailureCount;
    private final AtomicReference<WorkerWebUIOperations> mWorkerStatus;

    WorkerStatus() {
      mConsecutiveQuietCount = new LongAdder();
      mConsecutiveFailureCount = new LongAdder();
      mWorkerStatus = new AtomicReference<>(null);
    }

    void countWorkerIsQuiet() {
      mConsecutiveFailureCount.reset();
      mConsecutiveQuietCount.increment();
    }

    void countWorkerNotQuiet() {
      mConsecutiveFailureCount.reset();
      mConsecutiveQuietCount.reset();
    }

    void countError() {
      mConsecutiveQuietCount.reset();
      mConsecutiveFailureCount.increment();
    }

    boolean isWorkerQuiet() {
      return mConsecutiveQuietCount.sum() >= WORKER_QUIET_THRESHOLD;
    }

    boolean isWorkerInaccessible() {
      return mConsecutiveFailureCount.sum() >= WORKER_ERROR_THRESHOLD;
    }

    void recordWorkerStatus(WorkerWebUIOperations status) {
      mWorkerStatus.set(status);
    }

    WorkerWebUIOperations getWorkerTrackedStatus() {
      return mWorkerStatus.get();
    }
  }
}
