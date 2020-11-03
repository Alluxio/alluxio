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
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.DeletePOptions;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

/**
 * Moves a file or directory specified by args.
 */
public class DistributedMvCommand extends AbstractDistributedJobCommand {

  private DistributedCpCommand mCpCommand;

  /**
   * @param fsContext the filesystem context of Alluxio
   */
  public DistributedMvCommand(FileSystemContext fsContext) {
    super(fsContext);

    mCpCommand = new DistributedCpCommand(fsContext);
  }

  @Override
  public String getCommandName() {
    return "distributedMv";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    mCpCommand.validateArgs(cl);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    try {
      mCpCommand.run(cl);
    } catch (AlluxioException | IOException e) {
      System.out.println("Copy operation portion of Move failed. If the error below is "
          + "intermittent, you can rerun this by deleting the destination first.");
      throw e;
    }

    String[] args = cl.getArgs();
    AlluxioURI srcPath = new AlluxioURI(args[0]);

    // ## MigrateDeleteUnchecked
    // Delete the source unchecked because there is no guarantee that the source
    // has been fulled persisted yet if the source was written using ASYNC_THROUGH
    System.out.println("Deleting " + srcPath);
    mFileSystem.delete(srcPath,
        DeletePOptions.newBuilder().setUnchecked(true).setRecursive(true).build());
    return 0;
  }

  @Override
  public String getUsage() {
    return "distributedMv <src> <dst>";
  }

  @Override
  public String getDescription() {
    return "Moves a file or directory in parallel at file level";
  }
}
