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
import alluxio.wire.LineageInfo;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Lists all the lineages.
 */
@ThreadSafe
public final class ListLineagesCommand extends AbstractShellCommand {

  /**
   * Constructs a new instance to list all the lineages.
   *
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public ListLineagesCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "listLineages";
  }

  @Override
  protected int getNumOfArgs() {
    return 0;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    AlluxioLineage tl = AlluxioLineage.get();
    List<LineageInfo> infos = tl.getLineageInfoList();
    for (LineageInfo info : infos) {
      System.out.println(info);
    }
  }

  @Override
  public String getUsage() {
    return "listLineages";
  }

  @Override
  public String getDescription() {
    return "Lists all lineages.";
  }
}
