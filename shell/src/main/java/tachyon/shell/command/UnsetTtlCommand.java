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

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;

/**
 * Unsets the TTL value for the given path.
 */
public final class UnsetTtlCommand extends AbstractTfsShellCommand {

  public UnsetTtlCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
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
  public void run(String... args) throws IOException {
    TachyonURI inputPath = new TachyonURI(args[0]);
    CommandUtils.setTtl(mTfs, inputPath, Constants.NO_TTL);
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
