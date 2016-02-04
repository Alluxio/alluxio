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

import alluxio.TachyonURI;
import alluxio.client.file.FileSystem;
import alluxio.conf.TachyonConf;
import alluxio.exception.TachyonException;

/**
 * Unmounts a Tachyon path.
 */
@ThreadSafe
public final class UnmountCommand extends AbstractTfsShellCommand {
  /**
   * @param conf the configuration for Tachyon
   * @param fs the filesystem of Tachyon
   */
  public UnmountCommand(TachyonConf conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "unmount";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    TachyonURI inputPath = new TachyonURI(args[0]);

    try {
      mFileSystem.unmount(inputPath);
      System.out.println("Unmounted " + inputPath);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "unmount <tachyonPath>";
  }

  @Override
  public String getDescription() {
    return "Unmounts a Tachyon path.";
  }
}
