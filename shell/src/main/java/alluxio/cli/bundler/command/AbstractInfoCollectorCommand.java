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
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import org.apache.commons.cli.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * Abstraction of a command under InfoCollector.
 * */
public abstract class AbstractInfoCollectorCommand implements Command {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractInfoCollectorCommand.class);
  private static final String FILE_NAME_SUFFIX = ".txt";

  private FileSystemContext mFsContext;

  /**
   * Creates an instance of {@link AbstractInfoCollectorCommand}.
   *
   * @param fsContext {@link FileSystemContext} the context to run in
   * */
  public AbstractInfoCollectorCommand(@Nullable FileSystemContext fsContext) {
    if (fsContext == null) {
      fsContext =
              FileSystemContext.create(new InstancedConfiguration(ConfigurationUtils.defaults()));
    }
    mFsContext = fsContext;
  }

  /**
   * Get the destination dir to put work in.
   *
   * @param cl the {@link CommandLine} parsed from command line arguments
   * @return the destination dir specified by commandline arguments
   * */
  public static String getDestDir(CommandLine cl) {
    String[] args = cl.getArgs();
    return args[0];
  }

  /**
   * Gets the directory that this command should output to.
   * Creates the directory if it does not exist.
   * This will be a under the dest dir of
   * {@link AbstractInfoCollectorCommand#getDestDir(CommandLine)}.
   *
   * @param baseDirPath the base directory this command should output to
   * @return the directory path
   * */
  public String getWorkingDirectory(String baseDirPath) {
    String workingDirPath =  Paths.get(baseDirPath, this.getCommandName()).toString();
    // mkdirs checks existence of the path
    File workingDir = new File(workingDirPath);
    workingDir.mkdirs();
    return workingDirPath;
  }

  /**
   * Returns true if previous work of this command is found in the working directory.
   * This means the command can skip doing real work.
   *
   * @param baseDirPath the base directory this command should output to
   * @return whether previous work is found
   * */
  public boolean foundPreviousWork(String baseDirPath) {
    String workingDirPath = getWorkingDirectory(baseDirPath);
    // TODO(jiacheng): this is wrong!
    File workingDir = new File(workingDirPath);

    // TODO(jiacheng): better idea?
    // If the working directory is not empty, assume previous work can be reused.
    if (workingDir.list().length == 0) {
      return false;
    }
    LOG.info(String.format("Working dir %s is not empty. Assume previous work has completed.",
            workingDirPath));
    return true;
  }

  /**
   * Generates the output file for the command to write printouts to.
   *
   * @param baseDirPath the base directory this command should output to
   * @param fileName name of the output file
   * @return the output file
   * */
  public File generateOutputFile(String baseDirPath, String fileName) throws IOException {
    if (!fileName.endsWith(FILE_NAME_SUFFIX)) {
      fileName += FILE_NAME_SUFFIX;
    }
    String outputFilePath = Paths.get(getWorkingDirectory(baseDirPath), fileName).toString();
    File outputFile = new File(outputFilePath);
    if (!outputFile.exists()) {
      outputFile.createNewFile();
    }
    return outputFile;
  }

  private void createWorkingDirIfNotExisting(String path) {
    // mkdirs checks existence of the path
    File workingDir = new File(path);
    workingDir.mkdirs();
  }
}
