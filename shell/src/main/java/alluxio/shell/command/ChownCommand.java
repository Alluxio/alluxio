/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.shell.command;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import alluxio.TachyonURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.options.SetAttributeOptions;
import alluxio.conf.TachyonConf;
import alluxio.exception.TachyonException;

/**
 * Changes the owner of a file or directory specified by args.
 */
@ThreadSafe
public final class ChownCommand extends AbstractTfsShellCommand {

  /**
   * Creates a new instance of {@link ChownCommand}.
   *
   * @param conf a Tachyon configuration
   * @param fs a Tachyon file system handle
   */
  public ChownCommand(TachyonConf conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "chown";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  protected Options getOptions() {
    return new Options().addOption(RECURSIVE_OPTION);
  }

  /**
   * Changes the owner for the directory or file with the path specified in args.
   *
   * @param path The {@link TachyonURI} path as the input of the command
   * @param owner The owner to be updated to the file or directory
   * @param recursive Whether change the owner recursively
   * @throws IOException if command failed
   */
  private void chown(TachyonURI path, String owner, boolean recursive) throws IOException {
    try {
      SetAttributeOptions options = SetAttributeOptions.defaults()
          .setOwner(owner).setRecursive(recursive);
      mFileSystem.setAttribute(path, options);
      System.out.println("Changed owner of " + path + " to " + owner);
    } catch (TachyonException e) {
      throw new IOException("Failed to changed owner of " + path + " to " + owner + " : "
          + e.getMessage());
    }
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    String owner = args[0];
    TachyonURI path = new TachyonURI(args[1]);
    chown(path, owner, cl.hasOption("R"));
  }

  @Override
  public String getUsage() {
    return "chown -R <owner> <path>";
  }

  @Override
  public String getDescription() {
    return "Changes the owner of a file or directory specified by args."
        + " Specify -R to change the owner recursively.";
  }
}
