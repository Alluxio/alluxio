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
import alluxio.util.CommonUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;
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
        .addOption(V2_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    if (cl.hasOption(V2_OPTION.getOpt())) {
      DirectoryLoadPType loadPType = DirectoryLoadPType.valueOf(cl.getOptionValue(
          DIR_LOAD_TYPE_OPTION.getOpt(), "SINGLE_LISTING"));
      loadMetadataV2(plainPath, cl.hasOption(RECURSIVE_OPTION.getOpt()), loadPType,
          cl.hasOption(ASYNC_OPTION.getOpt()));
    } else {
      loadMetadata(plainPath, cl.hasOption(RECURSIVE_OPTION.getOpt()),
          cl.hasOption(FORCE_OPTION.getOpt()));
    }
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  private void loadMetadataV2(
      AlluxioURI path, boolean recursive, DirectoryLoadPType dirLoadType,
      boolean async) throws IOException {
    SyncMetadataPOptions options =
        SyncMetadataPOptions.newBuilder().setLoadDescendantType(recursive
            ? LoadDescendantPType.ALL : LoadDescendantPType.ONE)
            .setDirectoryLoadType(dirLoadType).build();
    if (!async) {
      try {
        SyncMetadataPResponse response = mFileSystem.syncMetadata(path, options);
        System.out.println("Sync metadata result: " + response);
        System.out.println(response.getDebugInfo());
        return;
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage());
      }
    }
    try {
      System.out.println("Submitting metadata sync task");
      SyncMetadataAsyncPResponse response = mFileSystem.syncMetadataAsync(path, options);
      long taskId = response.getTaskId();
      System.out.println("Task " + taskId + " submitted");
      while (true) {
        GetSyncProgressPResponse syncProgress = mFileSystem.getSyncProgress(taskId);
        if (syncProgress.getState() == GetSyncProgressPResponse.State.SUCCESS) {
          System.out.println("Sync succeeded");
          System.out.println(syncProgress.getDebugInfo());
          return;
        } else if (syncProgress.getState() == GetSyncProgressPResponse.State.FAIL) {
          System.out.println("Sync failed");
          return;
        }
        System.out.println(syncProgress.getNumFilesSynced());
        CommonUtils.sleepMs(2000);
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
    return "loadMetadata [-R] [-F] [-v2] [-a/--async] <path>";
  }

  @Override
  public String getDescription() {
    return "Loads metadata for the given Alluxio path from the under file system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
    if (cl.hasOption(FORCE_OPTION.getOpt()) && cl.hasOption(V2_OPTION.getOpt())) {
      throw new InvalidArgumentException("LoadMetadata v2 does not support -F option.");
    }
    if (cl.hasOption(ASYNC_OPTION.getOpt()) && !cl.hasOption(V2_OPTION.getOpt())) {
      throw new InvalidArgumentException("LoadMetadata v1 does not support -a/--async option.");
    }
  }
}
