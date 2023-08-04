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
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Copies the specified file specified by "source path" to the path specified by "remote path".
 * This command will fail if "remote path" already exists.
 */
@ThreadSafe
@PublicApi
public final class CopyFromLocalCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CopyFromLocalCommand.class);

  private final CpCommand mCpCommand;

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public CopyFromLocalCommand(FileSystemContext fsContext) {
    super(fsContext);
    mFsContext = fsContext;
    mFileSystem = FileSystem.Factory.create(fsContext);
    mCpCommand = new CpCommand(fsContext);
  }

  @Override
  public void close() throws IOException {
    // Close updated {@link FileSystem} instance that is created for internal cp command.
    // This will close the {@link FileSystemContext} associated with it.
    mFileSystem.close();
  }

  @Override
  public String getCommandName() {
    return "copyFromLocal";
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(CpCommand.THREAD_OPTION)
        .addOption(CpCommand.BUFFER_SIZE_OPTION);
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    mCpCommand.validateArgs(cl);
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
    return "copyFromLocal "
        + "[--thread <num>] "
        + "[--buffersize <bytes>] "
        + "<src> <remoteDst>";
  }

  @Override
  public String getDescription() {
    return "Copies a file or a directory from local filesystem to Alluxio filesystem "
        + "in parallel at file level.";
  }
}
