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

package alluxio.cli.fsadmin.metrics;

import alluxio.cli.CommandUtils;
import alluxio.cli.fsadmin.command.AbstractFsAdminCommand;
import alluxio.cli.fsadmin.command.Context;
import alluxio.client.block.AlluxioBlockStore;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.ClearMetricsRequest;
import alluxio.util.ThreadFactoryUtils;
import alluxio.wire.WorkerNetAddress;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Clear the leading master (and workers) metrics.
 */
public final class ClearCommand extends AbstractFsAdminCommand {
  private static final String MASTER_OPTION_NAME = "master";
  private static final String WORKERS_OPTION_NAME = "workers";
  private static final String SPECIFIED_OPTION_NAME = "node";
  private static final int DEFAULT_PARALLELISM = 8;

  private static final Option MASTER_OPTION =
      Option.builder()
          .longOpt(MASTER_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("Clear the metrics of Alluxio leading master")
          .build();
  private static final Option WORKERS_OPTION =
      Option.builder()
          .longOpt(WORKERS_OPTION_NAME)
          .required(false)
          .hasArg(false)
          .desc("Clear the metrics of all active workers")
          .build();
  private static final Option SPECIFIED_OPTION =
      Option.builder(SPECIFIED_OPTION_NAME)
          .required(false)
          .hasArg(true)
          .desc("Clear metrics of specified workers. "
              + "Pass in the worker hostnames separated by comma")
          .build();

  private final AlluxioConfiguration mAlluxioConf;

  /**
   * @param context fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public ClearCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mAlluxioConf = alluxioConf;
  }

  @Override
  public String getCommandName() {
    return "clear";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(MASTER_OPTION)
        .addOption(WORKERS_OPTION).addOption(SPECIFIED_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    Option[] options = cl.getOptions();
    if (options.length > 1) {
      System.out.println(getDescription());
      System.out.println(getUsage());
      System.out.println("Passed in too many options");
      return -1;
    }
    if (cl.hasOption(SPECIFIED_OPTION_NAME)) {
      String specifiedOptionValue = cl.getOptionValue(SPECIFIED_OPTION_NAME);
      Set<String> workerSet = new HashSet<>(Arrays.asList(specifiedOptionValue.split(",")));

      try (FileSystemContext context = FileSystemContext.create(mAlluxioConf)) {
        AlluxioBlockStore store = AlluxioBlockStore.create(FileSystemContext.create(mAlluxioConf));
        List<WorkerNetAddress> addressList = store.getEligibleWorkers().stream()
            .map(BlockWorkerInfo::getNetAddress).collect(Collectors.toList());
        List<WorkerNetAddress> workersToClear = new ArrayList<>();
        for (WorkerNetAddress worker : addressList) {
          if (workerSet.contains(worker.getHost())) {
            workersToClear.add(worker);
            workerSet.remove(worker.getHost());
          }
        }
        if (workerSet.size() != 0) {
          System.out.printf("Cannot find workers of hostnames %s%n", String.join(",", workerSet));
          return -1;
        }
        return clearWorkers(workersToClear, context) ? 0 : -1;
      }
    }

    if (options.length == 0 || cl.hasOption(WORKERS_OPTION_NAME)) {
      try (FileSystemContext context = FileSystemContext.create(mAlluxioConf)) {
        AlluxioBlockStore store = AlluxioBlockStore.create(FileSystemContext.create(mAlluxioConf));
        List<WorkerNetAddress> addressList = store.getEligibleWorkers().stream()
            .map(BlockWorkerInfo::getNetAddress).collect(Collectors.toList());
        if (!clearWorkers(addressList, context)) {
          return -1;
        }
      }
    }

    if (options.length == 0 || cl.hasOption(MASTER_OPTION_NAME)) {
      // Clear worker metrics before master metrics since worker metrics report
      // may be flaky during metrics clearance
      try {
        mMetricsClient.clearMetrics();
        System.out.printf("Successfully cleared metrics of Alluxio leading master.%n");
      } catch (Exception e) {
        System.out.println("Fatal error: " + e);
        return -1;
      }
    }
    return 0;
  }

  /**
   * Clears the metrics of a list of workers.
   *
   * @param workers the workers to clear metrics of
   * @param context FileSystemContext
   * @return true if clear succeed, false otherwise
   */
  private boolean clearWorkers(List<WorkerNetAddress> workers,
      FileSystemContext context) throws IOException {
    int workerNum = workers.size();
    if (workerNum == 0) {
      System.out.println("No worker metrics to clear.");
      return true;
    } else if (workerNum == 1) {
      clearWorkerMetrics(workers.get(0), context);
    } else {
      List<Future<Void>> futures = new ArrayList<>();
      int parallelism = Math.min(workerNum, DEFAULT_PARALLELISM);
      ExecutorService service = Executors.newFixedThreadPool(parallelism,
          ThreadFactoryUtils.build("metrics-clear-cli-%d", true));
      for (WorkerNetAddress worker : workers) {
        futures.add(service.submit(new ClearCallable(worker, context)));
      }
      try {
        for (Future<Void> future : futures) {
          future.get();
        }
      } catch (ExecutionException e) {
        System.out.println("Fatal error: " + e);
        return false;
      } catch (InterruptedException e) {
        System.out.println("Metrics clearance interrupted, exiting.");
        return false;
      } finally {
        service.shutdownNow();
      }
    }
    return true;
  }

  /**
   * Thread that polls a persist queue and persists files.
   */
  private class ClearCallable implements Callable<Void> {
    private final WorkerNetAddress mWorker;
    private final FileSystemContext mContext;

    ClearCallable(WorkerNetAddress worker, FileSystemContext context) {
      mWorker = worker;
      mContext = context;
    }

    @Override
    public Void call() throws Exception {
      clearWorkerMetrics(mWorker, mContext);
      return null;
    }
  }

  /**
   * Clears the worker metrics.
   *
   * @param worker the worker to clear metrics of
   * @param context the file system context
   */
  private void clearWorkerMetrics(WorkerNetAddress worker,
                                  FileSystemContext context) throws IOException {
    BlockWorkerClient blockWorkerClient = context.acquireBlockWorkerClient(worker);
    try {
      blockWorkerClient.clearMetrics(ClearMetricsRequest.newBuilder().build());
    } finally {
      context.releaseBlockWorkerClient(worker, blockWorkerClient);
    }
    System.out.printf("Successfully cleared metrics of worker %s.%n", worker.getHost());
  }

  @Override
  public String getUsage() {
    return String.format("%s [--%s|--%s|--%s <worker_hostnames>]%n"
            + "\t--%s: %s%n"
            + "\t--%s: %s%n"
            + "\t--%s: %s%n",
        getCommandName(), MASTER_OPTION_NAME, WORKERS_OPTION_NAME, SPECIFIED_OPTION_NAME,
        MASTER_OPTION_NAME, MASTER_OPTION.getDescription(),
        WORKERS_OPTION_NAME, WORKERS_OPTION.getDescription(),
        SPECIFIED_OPTION_NAME, SPECIFIED_OPTION.getDescription());
  }

  /**
   * @return command's description
   */
  @VisibleForTesting
  public static String description() {
    return "Clear the metrics of the whole cluster by default. "
        + "Users can pass in options to decide metrics of which nodes to be cleared. "
        + "This command is useful when getting metrics information in short-term testing. "
        + "This command should be used sparingly as it may affect the current metrics "
        + "recording and reporting which may lead to metrics incorrectness. ";
  }

  @Override
  public String getDescription() {
    return description();
  }
}
