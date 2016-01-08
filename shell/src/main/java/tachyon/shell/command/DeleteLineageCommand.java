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

import tachyon.client.file.FileSystem;
import tachyon.client.lineage.TachyonLineage;
import tachyon.client.lineage.options.DeleteLineageOptions;
import tachyon.conf.TachyonConf;

/**
 * Deletes a lineage by its id. If the cascade flag is set to true it performs a cascading delete.
 */
public final class DeleteLineageCommand extends AbstractTfsShellCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public DeleteLineageCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "deleteLineage";
  }

  @Override
  protected int getNumOfArgs() {
    return 2;
  }

  @Override
  public void run(String... args) throws IOException {
    TachyonLineage tl = TachyonLineage.get();
    long lineageId = Long.parseLong(args[0]);
    boolean cascade = Boolean.parseBoolean(args[1]);
    DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(cascade);
    try {
      tl.deleteLineage(lineageId, options);
    } catch (Exception e) {
      e.printStackTrace();
      System.out.println("Lineage '" + lineageId + "' could not be deleted.");
    }
    System.out.println("Lineage " + lineageId + " has been deleted.");
  }

  @Override
  public String getUsage() {
    return "deleteLineage <lineageId> <cascade(true|false)>";
  }
}
