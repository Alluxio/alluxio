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

package alluxio.cli.fs.command;

import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.lineage.AlluxioLineage;
import alluxio.client.lineage.LineageContext;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.wire.LineageInfo;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Lists all the lineages.
 */
@ThreadSafe
public final class ListLineagesCommand extends AbstractFileSystemCommand {

  /**
   * Constructs a new instance to list all the lineages.
   *
   * @param fs the filesystem of Alluxio
   */
  public ListLineagesCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "listLineages";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 0);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    AlluxioLineage tl = AlluxioLineage.get(LineageContext.INSTANCE);
    List<LineageInfo> infos = tl.getLineageInfoList();
    for (LineageInfo info : infos) {
      System.out.println(info);
    }
    return 0;
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
