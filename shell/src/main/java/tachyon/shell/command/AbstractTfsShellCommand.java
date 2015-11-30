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

import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;

/**
 * The base class for all the TfsShellCommand classes. It provides a default argument validation
 * method and a place to hold the TachyonFileSystemt client.
 */
public abstract class AbstractTfsShellCommand implements TfsShellCommand {

  protected TachyonConf mTachyonConf;
  protected TachyonFileSystem mTfs;

  protected AbstractTfsShellCommand(TachyonConf conf, TachyonFileSystem tfs) {
    mTachyonConf = conf;
    mTfs = tfs;
  }

  /**
   * Gets the expected number of arguments of the command.
   *
   * @return the number of arguments
   */
  abstract int getNumOfArgs();

  @Override
  public boolean validateArgs(String... args) {
    boolean valid = args.length == getNumOfArgs();
    if (!valid) {
      System.out.println(getCommandName() + " takes " + getNumOfArgs() + " arguments, "
              + " not " + args.length + "\n");
    }
    return valid;
  }
}
