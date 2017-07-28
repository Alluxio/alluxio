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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Creates a 0 byte file specified by argv. The file will be written to UnderFileSystem.
 */
@ThreadSafe
public final class TouchCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public TouchCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "touch";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);

    mFileSystem.createFile(inputPath, CreateFileOptions.defaults()).close();
    System.out.println(inputPath + " has been created");
    return 0;
  }

  @Override
  public String getUsage() {
    return "touch <path>";
  }

  @Override
  public String getDescription() {
    return "Creates a 0 byte file. The file will be written to the under file system.";
  }
}
