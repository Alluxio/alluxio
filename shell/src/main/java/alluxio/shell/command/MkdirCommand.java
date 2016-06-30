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
import alluxio.client.file.options.CreateDirectoryOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Creates a new directory specified by the path in args, including any parent folders that are
 * required. This command fails if a directory or file with the same path already exists.
 */
@ThreadSafe
public final class MkdirCommand extends AbstractShellCommand {

  /**
   * Constructs a new instance to create a new directory.
   *
   * @param fs the filesystem of Alluxio
   */
  public MkdirCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "mkdir";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    for (String path : args) {
      AlluxioURI inputPath = new AlluxioURI(path);

      CreateDirectoryOptions options = CreateDirectoryOptions.defaults().setRecursive(true);
      mFileSystem.createDirectory(inputPath, options);
      System.out.println("Successfully created directory " + inputPath);
    }
  }

  @Override
  public String getUsage() {
    return "mkdir <path1> [path2] ... [pathn]";
  }

  @Override
  public String getDescription() {
    return "Creates the specified directories, including any parent directories that are required.";
  }

  @Override
  public boolean validateArgs(String... args) {
    boolean valid = args.length >= getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes " + getNumOfArgs() + " argument at least\n");
    }
    return valid;
  }
}
