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

import alluxio.annotation.PublicApi;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies a file or a directory from the Alluxio filesystem to the local filesystem.
 */
@ThreadSafe
@PublicApi
public final class CopyToLocalCommand extends AbstractFileSystemCommand {

  private CpCommand mCpCommand;

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public CopyToLocalCommand(FileSystemContext fsContext) {
    super(fsContext);
    mCpCommand = new CpCommand(fsContext);
  }

  @Override
  public String getCommandName() {
    return "copyToLocal";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(CpCommand.BUFFER_SIZE_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    mCpCommand.validateArgs(cl);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String dst = args[1];
    cl.getArgList().set(1, "file://" + new File(dst).getAbsolutePath());
    mCpCommand.run(cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "copyToLocal "
        + "[--buffersize <bytes>] "
        + " <src> <localDst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory from the Alluxio filesystem to the local filesystem.";
  }
}
