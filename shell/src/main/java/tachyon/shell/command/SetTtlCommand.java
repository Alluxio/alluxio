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

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;

/**
 * Sets a new TTL value for the file at path both of the TTL value and the path
 * are specified by args.
 */
public final class SetTtlCommand extends AbstractTfsShellCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public SetTtlCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "setTtl";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public void run(String... args) throws IOException {
    long ttlMs = Long.parseLong(args[1]);
    Preconditions.checkArgument(ttlMs >= 0, "TTL value must be >= 0");
    TachyonURI path = new TachyonURI(args[0]);
    CommandUtils.setTtl(mTfs, path, ttlMs);
    System.out.println("TTL of file '" + path + "' was successfully set to " + ttlMs
        + " milliseconds.");
  }

  @Override
  public String getUsage() {
    return "setTtl <path> <time to live(in milliseconds)>";
  }

  @Override
  public String getDescription() {
    return "Sets a new TTL value for the file at path.";
  }
}
