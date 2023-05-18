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
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.DirectoryLoadPType;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetSyncProgressPResponse;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.SyncMetadataAsyncPResponse;
import alluxio.grpc.SyncMetadataPOptions;
import alluxio.grpc.SyncMetadataPResponse;
import alluxio.grpc.SyncMetadataState;
import alluxio.grpc.SyncMetadataTask;
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads metadata about a path in the UFS to Alluxio. No data will be transferred.
 * This command is a client-side optimization without storing all returned `ls`
 * results, preventing OOM for massive amount of small files.
 */
@ThreadSafe
@PublicApi
public class LoadMetadataCommand extends AbstractFileSystemCommand {
  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("load metadata subdirectories recursively")
          .build();

  private static final Option FORCE_OPTION =
      Option.builder("F")
          .required(false)
          .hasArg(false)
          .desc("update the metadata of the existing sub file forcibly")
          .build();

  private static final Option ASYNC_OPTION =
      Option.builder("a")
          .longOpt("async")
          .required(false)
          .hasArg(false)
          .desc("load the metadata asynchronously")
          .build();

  private static final Option V2_OPTION =
      Option.builder("v2")
          .required(false)
          .hasArg(false)
          .desc("use the load metadata v2 implementation")
          .build();

  private static final Option DIR_LOAD_TYPE_OPTION =
      Option.builder("d")
          .required(false)
          .hasArg()
          .desc("load directory type, can be SINGLE_LISTING, BFS, or DFS")
          .build();

  private static final Option OPERATION_OPTION =
      Option.builder("o")
          .required(false)
          .longOpt("option")
          .hasArg()
          .desc("operation, can be load, get, cancel")
          .build();

  private static final Option POLLING_OPTION =
      Option.builder("p")
          .required(false)
          .longOpt("polling")
          .hasArg()
          .desc("when running a task asynchronously, how often to poll the task progress in ms")
          .build();

  private static final Option TASK_ID_OPTION =
      Option.builder("id")
          .required(false)
          .hasArg()
          .desc("the numeric task group id")
          .build();

  private final List<String> mOperationValues = Arrays.asList("load", "get", "cancel");

  /**
   * Constructs a new instance to load metadata for the given Alluxio path from UFS.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public LoadMetadataCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "loadMetadata";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(RECURSIVE_OPTION)
        .addOption(FORCE_OPTION)
        .addOption(ASYNC_OPTION)
        .addOption(DIR_LOAD_TYPE_OPTION)
        .addOption(V2_OPTION)
        .addOption(OPERATION_OPTION)
        .addOption(POLLING_OPTION)
        .addOption(TASK_ID_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    String operation = cl.getOptionValue(OPERATION_OPTION.getOpt(), "load");
    if (operation.equals("get")) {
      getSyncProgress(Long.parseLong(cl.getOptionValue(TASK_ID_OPTION.getOpt())));
    } else if (operation.equals("cancel")) {
      cancel(Long.parseLong(cl.getOptionValue(TASK_ID_OPTION.getOpt())));
    } else if (cl.hasOption(V2_OPTION.getOpt())) {
      DirectoryLoadPType loadPType = DirectoryLoadPType.valueOf(cl.getOptionValue(
          DIR_LOAD_TYPE_OPTION.getOpt(), "SINGLE_LISTING"));
      loadMetadataV2(plainPath, cl.hasOption(RECURSIVE_OPTION.getOpt()), loadPType,
          cl.hasOption(ASYNC_OPTION.getOpt()),
          Integer.parseInt(cl.getOptionValue(POLLING_OPTION.getOpt(), "10000")));
    } else {
      loadMetadata(plainPath, cl.hasOption(RECURSIVE_OPTION.getOpt()),
          cl.hasOption(FORCE_OPTION.getOpt()));
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path;
    if (args.length > 0) {
      path = new AlluxioURI(args[0]);
      runWildCardCmd(path, cl);
    } else {
      // -o cancel [task_id] / -o get [task_id]
      runPlainPath(null, cl);
    }

    return 0;
  }

  private void printTask(SyncMetadataTask task) {
    System.out.println("Task id: " + task.getId());
    if (task.getState() == SyncMetadataState.SUCCEEDED) {
      System.out.println(Constants.ANSI_GREEN + "State: " + task.getState() + Constants.ANSI_RESET);
    } else if (task.getState() == SyncMetadataState.FAILED) {
      System.out.println(Constants.ANSI_RED + "State: " + task.getState() + Constants.ANSI_RESET);
    } else {
      System.out.println("State: " + task.getState());
    }
    System.out.println("Sync duration: " + task.getSyncDurationMs());
    double opsSec = task.getSyncDurationMs() == 0 ? 0
        : (double) task.getSuccessOpCount() / ((double) task.getSyncDurationMs() / (double) 1000);
    System.out.println("Ops/sec: " + opsSec);
    if (task.hasException()) {
      System.out.println(Constants.ANSI_RED + "Exception: " + Constants.ANSI_RESET);
      System.out.println(Constants.ANSI_RED + "\t" + task.getException().getExceptionType()
          + Constants.ANSI_RESET);
      System.out.println(Constants.ANSI_RED + "\t" + task.getException().getExceptionMessage()
          + Constants.ANSI_RESET);
      System.out.println(Constants.ANSI_RED + "\t" + task.getException().getStacktrace()
          + Constants.ANSI_RESET);
    }
    System.out.println("Task info: ");
    System.out.println("\t" + task.getTaskInfoString());
    System.out.println("Task stats: ");
    System.out.println("\t" + task.getTaskStatString());
    if (task.getState() == SyncMetadataState.SUCCEEDED) {
      System.out.println(Constants.ANSI_GREEN + "Load Metadata Completed." + Constants.ANSI_RESET);
    }
    if (task.getState() == SyncMetadataState.FAILED) {
      System.out.println(
          Constants.ANSI_RED + "Load Metadata Failed. Please check the server log or retry!"
              + Constants.ANSI_RESET);
    }
    if (task.getState() == SyncMetadataState.CANCELED) {
      System.out.println("Load Metadata Canceled.");
    }
  }

  private void getSyncProgress(long taskId) throws IOException, AlluxioException {
    GetSyncProgressPResponse syncProgress = mFileSystem.getSyncProgress(taskId);
    for (SyncMetadataTask task : syncProgress.getTaskList()) {
      printTask(task);
    }
  }

  private void cancel(long taskGroupId) throws IOException, AlluxioException {
    mFileSystem.cancelSyncMetadata(taskGroupId);
    System.out.println("Task group " + taskGroupId + " cancelled");
  }

  private void loadMetadataV2(
      AlluxioURI path, boolean recursive, DirectoryLoadPType dirLoadType,
      boolean async, long pollingIntervalMs) throws IOException {
    SyncMetadataPOptions options =
        SyncMetadataPOptions.newBuilder().setLoadDescendantType(recursive
                ? LoadDescendantPType.ALL : LoadDescendantPType.ONE)
            .setDirectoryLoadType(dirLoadType).build();
    if (!async) {
      try {
        System.out.println("Starting metadata sync..");
        SyncMetadataPResponse response = mFileSystem.syncMetadata(path, options);
        System.out.println("Sync Metadata finished");
        for (SyncMetadataTask task : response.getTaskList()) {
          printTask(task);
        }
        return;
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage());
      }
    }
    try {
      System.out.println("Submitting metadata sync task...");
      SyncMetadataAsyncPResponse response = mFileSystem.syncMetadataAsync(path, options);
      long taskGroupId = response.getTaskGroupId();
      System.out.println("Task group " + taskGroupId + " has been submitted successfully.");
      System.out.println("Task ids: " + Arrays.toString(response.getTaskIdsList().toArray()));
      System.out.println("Polling sync progress every " + pollingIntervalMs + "ms");
      System.out.println("You can also poll the sync progress in another terminal using:");
      System.out.println("\t$bin/alluxio fs loadMetadata -o get -id " + taskGroupId);
      System.out.println("Sync is being executed asynchronously. Ctrl+C or closing the terminal "
          + "does not stop the task group. To cancel the task, you can use: ");
      System.out.println("\t$bin/alluxio fs loadMetadata -o cancel -id " + taskGroupId);
      while (true) {
        System.out.println("------------------------------------------------------");
        GetSyncProgressPResponse syncProgress = mFileSystem.getSyncProgress(taskGroupId);
        List<SyncMetadataTask> tasks = syncProgress.getTaskList().stream()
            .sorted(Comparator.comparingLong(SyncMetadataTask::getId)).collect(Collectors.toList());
        boolean allComplete = true;
        System.out.println("Task group id: " + taskGroupId);
        for (SyncMetadataTask task : tasks) {
          printTask(task);
          if (task.getState() == SyncMetadataState.RUNNING) {
            allComplete = false;
          }
          System.out.println();
        }
        if (allComplete) {
          return;
        }
        CommonUtils.sleepMs(pollingIntervalMs);
      }
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  private void loadMetadata(AlluxioURI path, boolean recursive, boolean force) throws IOException {
    try {
      ListStatusPOptions options;
      if (force) {
        options = ListStatusPOptions.newBuilder()
            .setRecursive(recursive)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                .setSyncIntervalMs(0).build())
            .build();
      } else {
        options = ListStatusPOptions.newBuilder().setRecursive(recursive).build();
      }
      long time = CommonUtils.getCurrentMs();
      mFileSystem.loadMetadata(path, options);
      System.out.println("Time elapsed " + (CommonUtils.getCurrentMs() - time));
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return
        "loadMetadata [-R] [-F] [-v2] [-a/--async] [-o/--operation <operation>] "
            + "[-d <type>] [-p <polling interval ms>] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads metadata for the given Alluxio path from the under file system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    String operation = cl.getOptionValue(OPERATION_OPTION.getOpt(), "load");
    if (!mOperationValues.contains(operation)) {
      throw new InvalidArgumentException(
          "Operation value " + operation + " invalid. Possible values: load/cancel/get");
    }
    if (operation.equals("load")) {
      CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
    } else {
      CommandUtils.checkNumOfArgsNoMoreThan(this, cl, 0);
      if (cl.hasOption(FORCE_OPTION.getOpt())
          || cl.hasOption(ASYNC_OPTION.getOpt())
          || cl.hasOption(DIR_LOAD_TYPE_OPTION.getOpt())) {
        throw new InvalidArgumentException("-o load/cancel only supports -id option");
      }
      if (!cl.hasOption(TASK_ID_OPTION.getOpt())) {
        throw new InvalidArgumentException("-o load/cancel only comes with an -id option");
      }
    }
    if (cl.hasOption(FORCE_OPTION.getOpt()) && cl.hasOption(V2_OPTION.getOpt())) {
      throw new InvalidArgumentException("LoadMetadata v2 does not support -F option.");
    }
    if (cl.hasOption(ASYNC_OPTION.getOpt()) && !cl.hasOption(V2_OPTION.getOpt())) {
      throw new InvalidArgumentException("LoadMetadata v1 does not support -a/--async option.");
    }
    if (cl.hasOption(TASK_ID_OPTION.getOpt()) && operation.equals("load")) {
      throw new InvalidArgumentException(
          "-id option only works with get and cancel operation type");
    }
  }
}
