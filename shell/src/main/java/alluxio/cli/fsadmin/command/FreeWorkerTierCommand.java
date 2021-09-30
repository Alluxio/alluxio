package alluxio.cli.fsadmin.command;

import alluxio.annotation.PublicApi;
import alluxio.client.block.stream.BlockWorkerClient;
import alluxio.conf.AlluxioConfiguration;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.RemoveAllBlockRequest;
import alluxio.grpc.TryRemoveAllBlockRequest;
import alluxio.resource.CloseableResource;
import alluxio.wire.WorkerNetAddress;
import alluxio.exception.AlluxioException;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.SynchronousQueue;
import java.util.stream.Collectors;

/**
 * Command for free all worker managed cache space.
 */
@PublicApi
public class FreeWorkerTierCommand extends AbstractFsAdminCommand {
  /**
   * @param context     fsadmin command context
   * @param alluxioConf Alluxio configuration
   */
  public FreeWorkerTierCommand(Context context, AlluxioConfiguration alluxioConf) {
    super(context);
    mAlluxioConf = alluxioConf;
  }

  private static final Option HELP_OPTION =
      Option.builder("h")
          .longOpt("help")
          .required(false)
          .hasArg(false)
          .desc("help")
          .build();

  private static final Option WORKERS_OPTION =
      Option.builder("w")
          .longOpt("workers")
          .required(false)
          .hasArg(true)
          .desc("If given, free blocks from specified workers. "
              + "Pass in the worker hostnames separated by comma")
          .build();

  private static final Option FORCE_OPTION =
      Option.builder("f")
          .longOpt("force")
          .required(false)
          .hasArg(false)
          .desc("If given, "
              + "will remove all blocks that can be found in worker, whether persistent or not")
          .build();

  @Override
  public String getDescription() {
    return String.format("free(evict) all blocks from all worker \nusage: %s\n"
            + "Options:\n"
            + " -%s, --%s\t%s\n"
            + " -%s, --%s\t%s",
        getUsage(),
        FORCE_OPTION.getOpt(), FORCE_OPTION.getLongOpt(), FORCE_OPTION.getDescription(),
        WORKERS_OPTION.getOpt(), WORKERS_OPTION.getLongOpt(), WORKERS_OPTION.getDescription());
  }

  @Override
  public String getUsage() {
    return getCommandName() + " [--force] [--workers <worker_hostname>]";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(WORKERS_OPTION)
        .addOption(FORCE_OPTION)
        .addOption(HELP_OPTION);
  }

  private final AlluxioConfiguration mAlluxioConf;

  @Override
  public String getCommandName() {
    return "freeWorkerTier";
  }

  /**
   * Thread that free the block from a specific worker.
   */
  private class RemoveWorkerCallable implements Callable<Void> {
    private final WorkerNetAddress mWorker;
    private final FileSystemContext mContext;
    private final boolean mForce;

    RemoveWorkerCallable(WorkerNetAddress worker, FileSystemContext context, boolean force) {
      mWorker = worker;
      mContext = context;
      mForce = force;
    }

    @Override
    public Void call() throws Exception {
      try (CloseableResource<BlockWorkerClient> blockWorkerClient =
               mContext.acquireBlockWorkerClient(mWorker)) {
        if (mForce) {
          blockWorkerClient.get().removeAllBlock(RemoveAllBlockRequest.newBuilder().build());
        } else {
          blockWorkerClient.get().tryRemoveAllBlock(TryRemoveAllBlockRequest.newBuilder().build());
        }
      }
      System.out.printf("Successfully Free blocks of worker %s.%n", mWorker.getHost());
      return null;
    }
  }

  /**
   * Free all block of a list of workers.
   *
   * @param workers the workers to clear blocks of
   * @param context FileSystemContext
   * @param force   Whether to force all free block form worker,
   *                if true all blocks will be freed even if it is not persistent
   */
  private void freeWorkers(List<WorkerNetAddress> workers,
                           FileSystemContext context, boolean force) throws IOException {
    if (workers.isEmpty()) {
      return;
    }
    List<Future<Void>> futures = new ArrayList<>();
    ExecutorService service = new ThreadPoolExecutor(0, 32,
        0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
    for (WorkerNetAddress worker : workers) {
      futures.add(service.submit(new RemoveWorkerCallable(worker, context, force)));
    }
    try {
      for (Future<Void> future : futures) {
        future.get();
      }
    } catch (ExecutionException e) {
      System.out.println("Fatal error: " + e);
    } catch (InterruptedException e) {
      System.out.println("interrupted, exiting.");
    } finally {
      service.shutdownNow();
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    if (cl.hasOption(HELP_OPTION.getOpt())) {
      System.out.println(getDescription());
      return 0;
    }

    try (FileSystemContext context = FileSystemContext.create(mAlluxioConf)) {
      List<WorkerNetAddress> workersToClear = new ArrayList<>();
      List<WorkerNetAddress> allWorkersList = context.getCachedWorkers().stream()
          .map(BlockWorkerInfo::getNetAddress).collect(Collectors.toList());

      if (cl.hasOption(WORKERS_OPTION.getOpt())) {
        // get valid worker address and invalid worker address from command line
        String workersValue = cl.getOptionValue(WORKERS_OPTION.getOpt());
        Set<String> workersRequired = new HashSet<>(Arrays.asList(workersValue.split(",")));
        for (WorkerNetAddress worker : allWorkersList) {
          if (workersRequired.contains(worker.getHost())) {
            workersToClear.add(worker);
            workersRequired.remove(worker.getHost());
          }
        }
        if (!workersRequired.isEmpty()) {
          System.out.println("Wrong worker hostname ");
          System.out.println(" Invalid worker: " + String.join(", ", workersRequired));
          System.out.println(" Valid worker: " + workersToClear.stream().map(
              WorkerNetAddress::getHost).collect(Collectors.joining(", ")));
          System.out.println("Nothing to do");
          return -1;
        }
      } else {
        workersToClear = allWorkersList;
      }

      freeWorkers(workersToClear, context, cl.hasOption(FORCE_OPTION.getOpt()));

      return 0;
    }
  }
}
