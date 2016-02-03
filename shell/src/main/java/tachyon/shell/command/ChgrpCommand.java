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

package tachyon.shell.command;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import tachyon.TachyonURI;
import tachyon.client.file.FileSystem;
import tachyon.client.file.options.SetAclOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Changes the group of a file or directory specified by args.
 */
@ThreadSafe
public final class ChgrpCommand extends AbstractTfsShellCommand {

  /**
   * Creates a new instance of {@link ChgrpCommand}.
   *
   * @param conf a Tachyon configuration
   * @param fs a Tachyon file system handle
   */
  public ChgrpCommand(TachyonConf conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "chgrp";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  protected Options getOptions() {
    Options opts = new Options();
    // Add R option for recursively.
    Option recursive = Option.builder("R")
        .required(false)
        .hasArg(false)
        .desc("recusively")
        .build();

    opts.addOption(recursive);
    return opts;
  }

  /**
   * Changes the group for the directory or file with the path specified in args.
   *
   * @param path The {@link TachyonURI} path as the input of the command
   * @param group The group to be updated to the file or directory
   * @param recursive Whether change the group recursively
   * @throws IOException
   */
  private void chgrp(TachyonURI path, String group, boolean recursive) throws IOException {
    try {
      SetAclOptions options = SetAclOptions.defaults().setGroup(group).setRecursive(recursive);
      mFileSystem.setAcl(path, options);
      System.out.println("Changed group of " + path + " to " + group);
    } catch (TachyonException e) {
      throw new IOException("Failed to changed group of " + path + " to " + group + " : "
          + e.getMessage());
    }
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    String group = args[0];
    TachyonURI path = new TachyonURI(args[1]);

    chgrp(path, group, cl.hasOption("R"));
  }

  @Override
  public String getUsage() {
    return "chgrp [-R] <group> <path>";
  }

  @Override
  public String getDescription() {
    return "Changes the group of a file or directory specified by args. "
        + " Specify -R to change the group recursively.";
  }
}
