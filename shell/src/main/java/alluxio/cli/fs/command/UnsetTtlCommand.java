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
import alluxio.grpc.TtlAction;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Unsets the TTL value for the given path.
 */
@ThreadSafe
@PublicApi
public final class UnsetTtlCommand extends AbstractFileSystemCommand {

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public UnsetTtlCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "unsetTtl";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  protected void runPlainPath(AlluxioURI inputPath, CommandLine cl)
      throws AlluxioException, IOException {
    // Expiry doesn't matter in this case
    FileSystemCommandUtils.setTtl(mFileSystem, inputPath, Constants.NO_TTL, TtlAction.DELETE);
    System.out.println("TTL of file '" + inputPath + "' was successfully removed.");
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);
    runWildCardCmd(inputPath, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "unsetTtl <path>";
  }

  @Override
  public String getDescription() {
    return "Unsets the TTL value for the given path.";
  }
}
