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
import alluxio.grpc.LoadDescendantPType;
import alluxio.grpc.SyncMetadataPOptions;
import alluxio.grpc.SyncMetadataPResponse;

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
public class SyncMetadataCommand extends AbstractFileSystemCommand {
  private static final Option RECURSIVE_OPTION =
      Option.builder("R")
          .required(false)
          .hasArg(false)
          .desc("load metadata subdirectories recursively")
          .build();

  /**
   * Constructs a new instance to load metadata for the given Alluxio path from UFS.
   *
   * @param fsContext the filesystem of Alluxio
   */
  public SyncMetadataCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "syncMetadata";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(RECURSIVE_OPTION);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    syncMetadata(plainPath, cl.hasOption(RECURSIVE_OPTION.getOpt()));
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);

    return 0;
  }

  private void syncMetadata(AlluxioURI path, boolean recursive) throws IOException {
    try {
      SyncMetadataPOptions options =
          SyncMetadataPOptions.newBuilder().setLoadDescendantType(recursive
              ? LoadDescendantPType.ALL : LoadDescendantPType.ONE).build();
      SyncMetadataPResponse response = mFileSystem.syncMetadata(path, options);
      System.out.println("Sync metadata result: " + response);
      System.out.println(response.getDebugInfo());
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "syncMetadata [-R] <path>";
  }

  @Override
  public String getDescription() {
    return "Syncs metadata for the given Alluxio path from the under file system.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }
}
