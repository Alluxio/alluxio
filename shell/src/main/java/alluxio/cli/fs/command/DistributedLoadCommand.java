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

import alluxio.AlluxioURI;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.client.job.JobGrpcClientUtils;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.job.load.LoadConfig;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, makes it resident in memory.
 */
@ThreadSafe
public final class DistributedLoadCommand extends AbstractFileSystemCommand {
  public static final Option REPLICATION =
      Option.builder()
          .longOpt("replication")
          .required(false)
          .hasArg(true)
          .desc("number of replicas to have for each block of the loaded file")
          .build();
  public static final Option THREAD_OPTION =
      Option.builder()
          .longOpt("thread")
          .required(false)
          .hasArg(true)
          .numberOfArgs(1)
          .argName("threads")
          .type(Number.class)
          .desc("Number of mThreads used to load files in parallel, default value is CPU cores * 2")
          .build();

  private ThreadPoolExecutor mDistributedLoadServicePool;
  private CompletionService<AlluxioURI> mDistributedLoadService;
  private ArrayList<Future<AlluxioURI>> mFutures = new ArrayList<>();
  private int mThreads = Runtime.getRuntime().availableProcessors() * 2;

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public DistributedLoadCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "distributedLoad";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(REPLICATION)
        .addOption(THREAD_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    int replication = 1;
    if (cl.hasOption(REPLICATION.getLongOpt())) {
      replication = Integer.parseInt(cl.getOptionValue(REPLICATION.getLongOpt()));
    }
    if (cl.hasOption(THREAD_OPTION.getLongOpt())) {
      mThreads = Integer.parseInt(cl.getOptionValue(THREAD_OPTION.getLongOpt()));
    }
    mDistributedLoadServicePool = new ThreadPoolExecutor(mThreads, mThreads, 60,
        TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
    mDistributedLoadServicePool.allowCoreThreadTimeOut(true);
    mDistributedLoadService =
        new ExecutorCompletionService<>(mDistributedLoadServicePool);
    try {
      distributedLoad(path, replication);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return -1;
    }
    return 0;
  }

  /**
   * Create a new job to load a file in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   */
  private Callable<AlluxioURI> newJob(AlluxioURI filePath, int replication) {
    return new Callable<AlluxioURI>() {
      @Override
      public AlluxioURI call() throws Exception {
        JobGrpcClientUtils.run(new LoadConfig(filePath.getPath(), replication), 3,
            mFsContext.getPathConf(filePath));
        return filePath;
      }
    };
  }

  /**
   * Wait one job to complete.
   */
  private void waitJob() {
    while (true) {
      Future<AlluxioURI> future = null;
      try {
        // Take one completed job.
        future = mDistributedLoadService.take();
        if (future != null) {
          AlluxioURI uri = future.get();
          System.out.println(uri + " loaded");
          mFutures.remove(future);
          return;
        }
      } catch (ExecutionException e) {
        e.printStackTrace();
        mFutures.remove(future);
        return;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Distributed loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @param replication The replication of file to load into Alluxio memory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException      when non-Alluxio exception occurs
   */
  private void distributedLoad(AlluxioURI filePath, int replication)
      throws AlluxioException, IOException, InterruptedException {
    load(filePath, replication);
    // Wait remaining jobs to complete.
    while (!mFutures.isEmpty()) {
      waitJob();
    }
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException      when non-Alluxio exception occurs
   */
  private void load(AlluxioURI filePath, int replication)
      throws IOException, AlluxioException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        load(newPath, replication);
      }
    } else {
      if (status.getInAlluxioPercentage() == 100) {
        // The file has already been fully loaded into Alluxio.
        System.out.println(filePath + " already in Alluxio fully");
        return;
      }
      if (mFutures.size() >= mThreads) {
        // Wait one job to complete.
        waitJob();
      }
      Callable<AlluxioURI> call = newJob(filePath, replication);
      mFutures.add(mDistributedLoadService.submit(call));
      System.out.println(filePath + " loading");
    }
  }

  @Override
  public String getUsage() {
    return "distributedLoad [--replication <num>] [--thread <threads>] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, making it resident in memory.";
  }
}
