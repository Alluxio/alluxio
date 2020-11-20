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

package alluxio.cli.bundler.command;

import alluxio.cli.Command;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Abstraction of a command under CollectInfo.
 * */
public abstract class AbstractCollectInfoCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractCollectInfoCommand.class);

  protected FileSystemContext mFsContext;
  protected String mWorkingDirPath;

  /**
   * Creates an instance of {@link AbstractCollectInfoCommand}.
   *
   * @param fsContext {@link FileSystemContext} the context to run in
   * */
  public AbstractCollectInfoCommand(FileSystemContext fsContext) {
    mFsContext = fsContext;
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  /**
   * Gets the directory that this command should output to.
   * Creates the directory if it does not exist.
   *
   * @param cl the parsed {@link CommandLine}
   * @return the directory path
   * */
  public String getWorkingDirectory(CommandLine cl) {
    String[] args = cl.getArgs();
    String baseDirPath = args[1];
    String workingDirPath =  Paths.get(baseDirPath, this.getCommandName()).toString();
    LOG.debug("Command %s works in %s", this.getCommandName(), workingDirPath);
    // mkdirs checks existence of the path
    File workingDir = new File(workingDirPath);
    if (!workingDir.exists()) {

      System.out.format("Creating working directory: %s%n", workingDirPath);
      workingDir.mkdirs();
    }
    return workingDirPath;
  }

  /**
   * Generates the output file for the command to write printouts to.
   *
   * @param workingDirPath the base directory this command should output to
   * @param fileName name of the output file
   * @return the output file
   * */
  public File generateOutputFile(String workingDirPath, String fileName) throws IOException {
    String outputFilePath = Paths.get(workingDirPath, fileName).toString();
    File outputFile = new File(outputFilePath);
    if (!outputFile.exists()) {
      outputFile.createNewFile();
    }
    return outputFile;
  }
}
