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
import alluxio.client.file.FileSystem;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Displays the file's all blocks info.
 *
 * @deprecated since version 1.5
 */
@ThreadSafe
@Deprecated
public final class FileInfoCommand extends AbstractFileSystemCommand {
  /**
   * @param fs the filesystem of Alluxio
   */
  public FileInfoCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "fileInfo";
  }

  @Override
  public int run(CommandLine cl) {
    System.out
        .println("The \"alluxio fs fileInfo <path>\" command is deprecated since version 1.5.");
    System.out.println("Use the \"alluxio fs stat <path>\" command instead.");
    return 0;
  }

  @Override
  public String getUsage() {
    return "fileInfo <path>";
  }

  @Override
  public String getDescription() {
    return "Displays all block info for the specified file.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }
}
