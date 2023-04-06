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

package alluxio.cli.fs.command;

import alluxio.Constants;
import alluxio.client.block.BlockMasterClient;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.grpc.DecommissionWorkerPOptions;
import alluxio.retry.RetryPolicy;
import alluxio.retry.TimeBoundedRetry;
import alluxio.retry.TimeoutRetry;
import alluxio.util.SleepUtils;
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

/**
 * Decommission a specific worker, the decommissioned worker is not automatically
 * shutdown and are not chosen for writing new replicas.
 */
public final class DecommissionWorkerCommand extends AbstractFileSystemCommand {
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

  /**
   * Constructs a new instance to decommission the given worker from Alluxio.
   * @param fsContext the filesystem of Alluxio
   */
  public DecommissionWorkerCommand(FileSystemContext fsContext) {
    super(fsContext);
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
        String host = p[0];
        String port = p[1];
        WorkerNetAddress addr = new WorkerNetAddress().setHost(host).setWebPort(Integer.getInteger(port));
        result.add(addr);
      } else {
        // Assume the whole string is hostname
        int port = Configuration.getInt(PropertyKey.WORKER_WEB_PORT);
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
    List<BlockWorkerInfo> cachedWorkers = mFsContext.getCachedWorkers();

    Set<WorkerNetAddress> failedWorkers = new HashSet<>();
    Map<WorkerNetAddress, LongAdder> waitingWorkers = new HashMap<>();
    Set<WorkerNetAddress> finishedWorkers = new HashSet<>();
    for (WorkerNetAddress a : addresses) {
      System.out.println("Decommissioning worker on " + a.getHost());

      BlockWorkerInfo worker = findMatchingWorkerAddress(a, cachedWorkers);
      DecommissionWorkerPOptions options =
              DecommissionWorkerPOptions.newBuilder()
                      .setWorkerName(worker.getNetAddress().getHost()).build();

      try (CloseableResource<BlockMasterClient> blockMasterClient =
                   mFsContext.acquireBlockMasterClientResource()) {
        // TODO(jiacheng): Add a response to this rpc for additional info?
        blockMasterClient.get().decommissionWorker(options);
        System.out.format("Decommissioned worker %s on master%n", worker.getNetAddress());
        waitingWorkers.put(worker.getNetAddress(), new LongAdder());
      } catch (IOException ie) {
        System.err.format("Failed to decommission worker %s:%n", worker.getNetAddress());
        ie.printStackTrace();
        failedWorkers.add(worker.getNetAddress());
      }
    }
    System.out.format("Sent decommission messages to the master, %s failed and %s succeeded",
            failedWorkers, waitingWorkers);
    if (waitingWorkers.size() == 0) {
      System.out.println("Failed to decommission all workers on the master. The admin should check the worker hostnames.");
      return 1;
    }

    verifyFromMasterAndWait(waitingWorkers.keySet());

    // We blocking wait for the workers to quiet down, so when this command returns without error,
    // the admin is safe to proceed to stopping those workers
    Instant startWaiting = Instant.now();
    RetryPolicy retry = new TimeoutRetry(startWaiting.toEpochMilli() + waitTimeMs, 1000);
    while (waitingWorkers.size() > 0 && retry.attempt()) {
      // Poll the status from each worker
      for (Map.Entry<WorkerNetAddress, LongAdder> entry : waitingWorkers.entrySet()) {
        WorkerNetAddress address = entry.getKey();
        System.out.format("Polling status from worker %s:%s%n", address.getHost(), address.getWebPort());
        try {
          if (canWorkerBeStopped(address)) {
            entry.getValue().increment();
          }
        } catch (Exception e) {
          System.err.format("Failed to poll progress from worker %s%n", address.getHost());
          System.err.println("There are many reasons why the poll can fail, including but not limited to:");
          System.err.println("1. Worker is running with a low version which does not contain this endpoint");
          System.err.println("2. alluxio.worker.web.port is not configured correctly or is not accessible");
          System.err.println("3. Some transient I/O error");
          e.printStackTrace();
          // TODO(Jiacheng): consider moving this to failedWorkers, now it's an endless retry
        }
      }

      waitingWorkers.entrySet().removeIf(entry -> {
        boolean isQuiet = entry.getValue().sum() >= 3;
        if (isQuiet) {
          System.out.format("There is no operation on worker %s for 3 times in a row. Worker is considered safe to stop.", entry.getKey());
          finishedWorkers.add(entry.getKey());
        }
        return isQuiet;
      });
    }

    Instant end = Instant.now();
    System.out.format("Waited %s for all workers", Duration.between(startWaiting, end));
    if (waitingWorkers.size() > 0) {
      System.out.format("%s workers still have not finished all their operations%n", waitingWorkers.keySet());
      System.out.println("The admin should manually intervene and check those workers, before shutting them down.");
      System.out.format("%s workers finished all their operations successfully: %s%n", finishedWorkers.size(), finishedWorkers);
      return 1;
    } else {
      System.out.println("Finished polling workers " + finishedWorkers);
      return 0;
    }
  }

  // We verify the target workers have been taken off the list on the master
  // Then we manually block for a while so clients/proxies in the cluster all get the update
  private void verifyFromMasterAndWait(Collection<WorkerNetAddress> removedWorkers) {
    // Wait a while so the proxy instances will get updated worker list from master
    long workerListLag = Configuration.getMs(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL);
    System.out.format("Clients take %s to be updated on the new worker list so this command will "
                    + "block for the same amount of time to ensure the update propagates to clients in the cluster.%n",
            Configuration.get(PropertyKey.USER_WORKER_LIST_REFRESH_INTERVAL));
    SleepUtils.sleepMs(workerListLag);

    // Poll the latest worker list and verify the workers are decommissioned
    System.out.println("Verify the decommission has taken effect by listing all available workers on the master");
    try {
      // TODO(jiacheng): add a way to force refresh the worker list so we can verify before wait
      Set<BlockWorkerInfo> cachedWorkers = new HashSet<>(mFsContext.getCachedWorkers());
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

  // TODO(jiacheng): use NetworkAddressUtils so localhost and 0.0.0.0 can be resolved
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
      // TODO(jiacheng): consider short circuit
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
    return new Options().addOption(ADDRESSES_OPTION).addOption(WAIT_OPTION);
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
