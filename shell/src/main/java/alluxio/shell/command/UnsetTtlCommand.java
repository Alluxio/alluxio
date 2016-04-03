/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.client.file.FileSystem;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Unsets the TTL value for the given path.
 */
@ThreadSafe
public final class UnsetTtlCommand extends AbstractShellCommand {
  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public UnsetTtlCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "unsetTtl";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);
    CommandUtils.setTtl(mFileSystem, inputPath, Constants.NO_TTL);
    System.out.println("TTL of file '" + inputPath + "' was successfully removed.");
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
