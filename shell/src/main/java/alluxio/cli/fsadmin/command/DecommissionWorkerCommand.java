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
import alluxio.cli.fs.command.AbstractFileSystemCommand;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeBoundedRetry;
import alluxio.retry.TimeoutRetry;
import alluxio.util.SleepUtils;
import alluxio.util.network.NetworkAddressUtils;
import alluxio.wire.WorkerNetAddress;
import alluxio.resource.CloseableResource;

import alluxio.util.FormatUtils;
import alluxio.util.network.HttpUtils;
import alluxio.wire.WorkerWebUIOperations;
import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Decommission a specific worker, the decommissioned worker is not automatically
 * shutdown and are not chosen for writing new replicas.
 */
public final class DecommissionWorkerCommand extends AbstractFsAdminCommand {
  private static final Option ADDRESSES_OPTION =
      Option.builder("h")
          .longOpt("addresses")
          .required(true)  // Host option is mandatory.
          .hasArg(true)
          .numberOfArgs(1)
          .argName("addresses")
              // TODO(jiacheng): this takes web port instead of RPC port!
          .desc("One or more worker addresses separated by comma. If port is not specified, "
              + PropertyKey.WORKER_WEB_PORT.getName() + " will be used.")
          .build();
  private static final Option WAIT_OPTION =
          Option.builder("w")
                  .longOpt("wait")
                  .required(false)
                  .hasArg(true)
                  .numberOfArgs(1)
                  .argName("wait")
                  .desc("Time to wait, in human readable form like 5m.")
                  .build();
  // TODO(jiacheng): a better name for this
  private static final Option REJECT_OPTION =
          Option.builder("j")
                  .longOpt("reject")
                  .required(false)
                  .hasArg(false)
                  .argName("reject")
                  .desc("Time to wait, in human readable form like 5m.")
                  .build();


  /**
   * Constructs a new instance to decommission the given worker from Alluxio.
   * @param context the context containing all operator handles
   */
  private final AlluxioConfiguration mConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public DecommissionWorkerCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mConf = alluxioConf;
  }

  private List<WorkerNetAddress> getWorkerAddresses(CommandLine cl) {
    String workerAddressesStr = cl.getOptionValue(ADDRESSES_OPTION.getLongOpt());
    if (workerAddressesStr.isEmpty()) {
      throw new IllegalArgumentException("Worker addresses must be specified");
    }

    List<WorkerNetAddress> result = new ArrayList<>();
    for (String part : workerAddressesStr.split(",")) {
      if (part.contains(":")) {
        String[] p = part.split(":");
        Preconditions.checkState(p.length == 2, "worker address %s cannot be recognized", part);
        // TODO(jiacheng): handle hostnames like localhost
        String port = p[1];
        System.out.format("Decommissioning worker at %s:%s%n", p[0], Integer.parseInt(port));
        WorkerNetAddress addr = new WorkerNetAddress().setHost(p[0]).setWebPort(Integer.parseInt(port));
        result.add(addr);
      } else {
        int port = mConf.getInt(PropertyKey.WORKER_WEB_PORT);
        System.out.format("Decommissioning worker at %s:%s%n", part, port);
        WorkerNetAddress addr = new WorkerNetAddress().setHost(part).setWebPort(port);
        result.add(addr);
      }
    }
    return result;
  }

  private BlockWorkerInfo findMatchingWorkerAddress(WorkerNetAddress address, List<BlockWorkerInfo> cachedWorkers) {
    for (BlockWorkerInfo worker : cachedWorkers) {
      if (worker.getNetAddress().getHost().equals(address.getHost())) {
        return worker;
      }
    }

    throw new IllegalArgumentException("Worker " + address.getHost() + " is not known by the master. "
        + "Please check the hostname or retry later. Available workers are: " + printCachedWorkerAddresses(cachedWorkers));
  }

  private String printCachedWorkerAddresses(List<BlockWorkerInfo> cachedWorkers) {
    StringBuilder sb = new StringBuilder();
    for (BlockWorkerInfo blockWorkerInfo : cachedWorkers) {
      sb.append("\t").append(blockWorkerInfo.getNetAddress().getHost()).append(":")
          .append(blockWorkerInfo.getNetAddress().getRpcPort());
    }
    return sb.toString();
  }

  private long parseWaitTimeMs(CommandLine cl) {
    if (cl.hasOption(WAIT_OPTION.getLongOpt())) {
      String waitTimeStr = cl.getOptionValue(WAIT_OPTION.getLongOpt());
      return FormatUtils.parseTimeSize(waitTimeStr);
    } else {
      // TODO(jiacheng): constant
      return 5 * 60 * 1000; // 5min by default
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    List<WorkerNetAddress> addresses = getWorkerAddresses(cl);
    long waitTimeMs = parseWaitTimeMs(cl);
    FileSystemContext context = FileSystemContext.create();
    List<BlockWorkerInfo> cachedWorkers = context.getCachedWorkers();

    boolean canRegisterAgain = !cl.hasOption(REJECT_OPTION.getLongOpt());
    Set<WorkerNetAddress> failedWorkers = new HashSet<>();
    Map<WorkerNetAddress, WorkerStatus> waitingWorkers = new HashMap<>();
    Set<WorkerNetAddress> finishedWorkers = new HashSet<>();
    for (WorkerNetAddress a : addresses) {
      System.out.format("Decommissioning worker %s:%s%n", a.getHost(), a.getWebPort());

      BlockWorkerInfo worker = findMatchingWorkerAddress(a, cachedWorkers);
      WorkerNetAddress workerAddress = worker.getNetAddress();
      DecommissionWorkerPOptions options =
          DecommissionWorkerPOptions.newBuilder()
              .setWorkerHostname(workerAddress.getHost())
              .setWorkerWebPort(workerAddress.getWebPort())
              .setCanRegisterAgain(canRegisterAgain).build();
      try {
        mBlockClient.decommissionWorker(options);
        System.out.format("Set worker %s:%s decommissioned on master%n", workerAddress.getHost(), workerAddress.getWebPort());
        // Start counting for this worker
        waitingWorkers.put(worker.getNetAddress(), new WorkerStatus());
      } catch (IOException ie) {
        System.err.format("Failed to decommission worker %s:%s%n", workerAddress.getHost(), workerAddress.getWebPort());
        ie.printStackTrace();
        failedWorkers.add(workerAddress);
      }
    }
    System.out.format("Sent decommission messages to the master, %s failed and %s succeeded%n",
            failedWorkers.size(), waitingWorkers.size());
    System.out.format("Failed ones: %s%n", failedWorkers.stream()
        .map(w -> w.getHost() + ":" + w.getWebPort()).collect(Collectors.toList()));
    if (waitingWorkers.size() == 0) {
      System.out.println("Failed to decommission all workers on the master. The admin should check the worker hostnames.");
      return 1;
    }

    verifyFromMasterAndWait(context, waitingWorkers.keySet());

    // We block and wait for the workers to quiet down, so when this command returns without error,
    // the admin is safe to proceed to stopping those workers
    boolean helpPrinted = false;
    Instant startWaiting = Instant.now();
    Set<WorkerNetAddress> lostWorkers = new HashSet<>();
    RetryPolicy retry = new TimeoutRetry(startWaiting.toEpochMilli() + waitTimeMs, 1000);
    while (waitingWorkers.size() > 0 && retry.attempt()) {
      // Poll the status from each worker
      for (Map.Entry<WorkerNetAddress, WorkerStatus> entry : waitingWorkers.entrySet()) {
        WorkerNetAddress address = entry.getKey();
        System.out.format("Polling status from worker %s:%s%n", address.getHost(), address.getWebPort());
        try {
          if (canWorkerBeStopped(address)) {
            entry.getValue().countWorkerIsQuiet();
          } else {
            /*
             * If there are operations on the worker, clear the counter.
             * The worker is considered quiet only if there are zero operations in consecutive checks.
             */
            entry.getValue().countWorkerNotQuiet();
          }
        } catch (Exception e) {
          System.err.format("Failed to poll progress from worker %s%n", address.getHost());
          if (!helpPrinted) {
            System.err.println("There are many reasons why the poll can fail, including but not limited to:");
            System.err.println("1. Worker is running with a low version which does not contain this endpoint");
            System.err.println("2. alluxio.worker.web.port is not configured correctly or is not accessible");
            System.err.println("3. Some transient I/O error");
            helpPrinted = true;
          }
          e.printStackTrace();
          entry.getValue().countError();
        }
      }

      waitingWorkers.entrySet().removeIf(entry -> {
        boolean isQuiet = entry.getValue().isWorkerQuiet();
        if (isQuiet) {
          System.out.format("There is no operation on worker %s:%s for %s times in a row. Worker is considered safe to stop.",
              entry.getKey().getHost(), entry.getKey().getWebPort(), WorkerStatus.WORKER_QUIET_THRESHOLD);
          finishedWorkers.add(entry.getKey());
          return true;
        }
        boolean isError = entry.getValue().isWorkerInaccessible();
        if (isError) {
          System.out.format("Failed to poll status from worker %s:%s for %s times in a row. "
              + "Worker is considered inaccessible and not functioning. "
              + "If the worker is not functioning, it probably does not currently have ongoing I/O and can be stopped. "
              + "But the admin is advised to manually check the working before stopping it. %n",
              entry.getKey().getHost(), entry.getKey().getWebPort(), WorkerStatus.WORKER_ERROR_THRESHOLD);
          lostWorkers.add(entry.getKey());
          return true;
        }
        return false;
      });
    }

    Instant end = Instant.now();
    System.out.format("Waited %s minutes operations to quiet down on all workers%n", Duration.between(startWaiting, end).toMinutes());
    if (waitingWorkers.size() > 0 || lostWorkers.size() > 0) {
      System.out.format("%s workers still have not finished all their operations%n", waitingWorkers.keySet());
      System.out.println("The admin should manually intervene and check those workers, before shutting them down.");
      System.out.format("%s workers finished all their operations successfully: %s%n",
          finishedWorkers.size(), finishedWorkers.stream().map(WorkerNetAddress::getHost).collect(Collectors.toList()));
      System.out.format("%s workers became inaccessible and we assume there are no operations: %s%n",
          lostWorkers.size(), lostWorkers.stream().map(WorkerNetAddress::getHost).collect(Collectors.toList()));
      return 1;
    } else {
      System.out.println("There is no operation running on all workers. Decommission is successful");
      return 0;
    }
  }

  public static class WorkerStatus {
    public static final int WORKER_QUIET_THRESHOLD = 20;
    public static final int WORKER_ERROR_THRESHOLD = 5;

    public final LongAdder mConsecutiveQuietCount;
    public final LongAdder mConsecutiveFailureCount;
    public WorkerStatus() {
      mConsecutiveQuietCount = new LongAdder();
      mConsecutiveFailureCount = new LongAdder();
    }

    public void countWorkerIsQuiet() {
      mConsecutiveFailureCount.reset();
      mConsecutiveQuietCount.increment();
    }

    public void countWorkerNotQuiet() {
      mConsecutiveFailureCount.reset();
      mConsecutiveQuietCount.reset();
    }

    public void countError() {
      mConsecutiveQuietCount.reset();
      mConsecutiveFailureCount.increment();
    }

    public boolean isWorkerQuiet() {
      return mConsecutiveQuietCount.sum() >= WORKER_QUIET_THRESHOLD;
    }

    public boolean isWorkerInaccessible() {
      return mConsecutiveFailureCount.sum() >= WORKER_ERROR_THRESHOLD;
    }
  }

  // We verify the target workers have been taken off the list on the master
  // Then we manually block for a while so clients/proxies in the cluster all get the update
  private void verifyFromMasterAndWait(FileSystemContext context, Collection<WorkerNetAddress> removedWorkers) {
    // Wait a while so the proxy instances will get updated worker list from master
    long workerListLag = mConf.getMs(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL);
    System.out.format("Clients take %s to be updated on the new worker list so this command will "
                    + "block for the same amount of time to ensure the update propagates to clients in the cluster.%n",
            mConf.get(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL));
    SleepUtils.sleepMs(workerListLag);

    // Poll the latest worker list and verify the workers are decommissioned
    System.out.println("Verify the decommission has taken effect by listing all available workers on the master");
    try {
      // TODO(jiacheng): add a way to force refresh the worker list so we can verify before wait
      Set<BlockWorkerInfo> cachedWorkers = new HashSet<>(context.getCachedWorkers());
      System.out.println("Now on master the available workers are: ");
      System.out.println(cachedWorkers.stream().map(w -> {
        WorkerNetAddress address = w.getNetAddress();
        return address.getHost() + ":" + address.getWebPort();
      }).collect(Collectors.toList()));
      for (WorkerNetAddress addr : removedWorkers) {
        if (cachedWorkers.contains(addr)) {
          System.err.format("Worker %s is still showing available on the master. Please check why the decommission did not work.%n", addr);
          System.err.println("This command will still continue, but the admin should manually verify the state of this worker afterwards.");
        }
      }
    } catch (IOException e) {
      System.err.println("Failed to verify the latest available worker list from the master");
      e.printStackTrace();
    }
  }

  private boolean canWorkerBeStopped(WorkerNetAddress worker) throws IOException {
    URIBuilder uriBuilder = new URIBuilder();
    uriBuilder.setScheme("http");
    uriBuilder.setHost(worker.getHost());
    uriBuilder.setPort(worker.getWebPort());
    uriBuilder.setPath(Constants.REST_API_PREFIX + "/worker/operations");
    System.out.format("Polling worker on %s%n", uriBuilder);

    // Poll the worker status endpoint
    AtomicReference<WorkerWebUIOperations> workerState = new AtomicReference<>();
    HttpUtils.get(uriBuilder.toString(), 5000, inputStream -> {
      ObjectMapper mapper = new ObjectMapper();
      workerState.set(mapper.readValue(inputStream, WorkerWebUIOperations.class));
      System.out.println("Worker status is " + workerState.get());
    });

    if (workerState.get() == null) {
      // Should not reach here
      throw new IOException("Received null from worker operation status!");
    }
    WorkerWebUIOperations operationState = workerState.get();
    if (operationState.getOperationCount() == 0) {
      // TODO(jiacheng): test what happens if worker is stopped during a short circuit read
      return true;
    }
    return false;
  }

  @Override
  public String getCommandName() {
    return "decommissionWorker";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(ADDRESSES_OPTION).addOption(WAIT_OPTION).addOption(REJECT_OPTION);
  }

  @Override
  public String getUsage() {
    return "decommissionWorker --h <worker host>";
  }

  @Override
  public String getDescription() {
    return "Decommission a specific worker in the Alluxio cluster. The decommissioned"
        + "worker is not shut down but will not accept new read/write operations. The ongoing "
        + "operations will proceed until completion.";
  }
}
