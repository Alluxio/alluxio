/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.client.file.FileSystem;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.client.lineage.LineageContext;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Deletes a lineage by its id. If the cascade flag is set to true it performs a cascading delete.
 */
@ThreadSafe
public final class DeleteLineageCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public DeleteLineageCommand(FileSystem fs) {
    super(fs);
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
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioLineage tl = AlluxioLineage.get(LineageContext.INSTANCE);
    long lineageId = Long.parseLong(args[0]);
    boolean cascade = Boolean.parseBoolean(args[1]);
    DeleteLineageOptions options = DeleteLineageOptions.defaults().setCascade(cascade);
    tl.deleteLineage(lineageId, options);
    System.out.println("Lineage " + lineageId + " has been deleted.");
    return 0;
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
