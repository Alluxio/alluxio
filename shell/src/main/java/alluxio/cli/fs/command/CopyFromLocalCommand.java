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

import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import javax.annotation.concurrent.ThreadSafe;
import java.io.File;
import java.io.IOException;

/**
 * Copies the specified file specified by "source path" to the path specified by "remote path".
 * This command will fail if "remote path" already exists.
 */
@ThreadSafe
public final class CopyFromLocalCommand extends AbstractFileSystemCommand {

  private CpCommand mCpCommand;

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public CopyFromLocalCommand(FileSystemContext fsContext) {
    super(fsContext);
    mCpCommand = new CpCommand(fsContext);
  }

  @Override
  public String getCommandName() {
    return "copyFromLocal";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(CpCommand.THREAD_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 2);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    String srcPath = args[0];
    cl.getArgList().set(0, "file://" + new File(srcPath).getAbsolutePath());
    mCpCommand.run(cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "copyFromLocal [-t <number of threads for copying>] <src> <remoteDst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory from local filesystem to Alluxio filesystem "
        + "in parallel at file level.";
  }
}
