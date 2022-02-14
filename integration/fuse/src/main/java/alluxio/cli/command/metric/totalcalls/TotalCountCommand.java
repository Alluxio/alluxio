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

package alluxio.cli.command.metric.totalcalls;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.cli.command.AbstractFuseShellCommand;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.AlluxioConfiguration;
import alluxio.metrics.MetricKey;
import alluxio.metrics.MetricsSystem;
import alluxio.wire.FileInfo;

/**
 * The count subcommand of totalCalls, get the total calls count.
 */
public final class TotalCountCommand extends AbstractFuseShellCommand {

  /**
   * @param fs the file system the command takes effect on
   * @param conf the Alluxio configuration
   * @param parentCommandName the parent command name
   */
  public TotalCountCommand(FileSystem fs, AlluxioConfiguration conf, String parentCommandName) {
    super(fs, conf, parentCommandName);
  }

  @Override
  public String getCommandName() {
    return "count";
  }

  @Override
  public String getUsage() {
    return String.format("%s%s.fusemetric.%s.%s", Constants.DEAFULT_FUSE_MOUNT,
        Constants.ALLUXIO_CLI_PATH, getParentCommandName(), getCommandName());
  }

  @Override
  public URIStatus run(AlluxioURI path, String [] argv) {
    // The 'ls -l' command will show total calls count in the <filesize> field.
    long count = MetricsSystem.timer(MetricKey.FUSE_TOTAL_CALLS.getName()).getCount();
    return new URIStatus(new FileInfo().setLength(count).setCompleted(true));
  }

  @Override
  public String getDescription() {
    return "Get total calls count metric.";
  }
}
