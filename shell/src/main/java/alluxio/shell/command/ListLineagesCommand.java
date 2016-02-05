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
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;

import alluxio.client.file.FileSystem;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.Configuration;
import alluxio.wire.LineageInfo;

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
