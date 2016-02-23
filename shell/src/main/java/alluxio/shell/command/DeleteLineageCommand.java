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

import alluxio.Configuration;
import alluxio.client.file.FileSystem;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.client.lineage.options.DeleteLineageOptions;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Deletes a lineage by its id. If the cascade flag is set to true it performs a cascading delete.
 */
@ThreadSafe
public final class DeleteLineageCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public DeleteLineageCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
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
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    AlluxioLineage tl = AlluxioLineage.get();
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

  @Override
  public String getDescription() {
    return "Deletes a lineage. If cascade is specified as true, "
      + "dependent lineages will also be deleted.";
  }
}
