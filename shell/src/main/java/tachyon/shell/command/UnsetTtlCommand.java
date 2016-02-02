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

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.FileSystem;
import tachyon.conf.TachyonConf;

/**
 * Unsets the TTL value for the given path.
 */
@ThreadSafe
public final class UnsetTtlCommand extends AbstractTfsShellCommand {
  /**
   * @param conf the configuration for Tachyon
   * @param fs the filesystem of Tachyon
   */
  public UnsetTtlCommand(TachyonConf conf, FileSystem fs) {
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
    TachyonURI inputPath = new TachyonURI(args[0]);
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
