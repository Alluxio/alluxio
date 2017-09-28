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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import java.io.Serializable;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The lineage command-line job information.
 */
@PublicApi
@NotThreadSafe
// TODO(jiri): Consolidate with alluxio.job.CommandLineJob.
public final class CommandLineJobInfo implements Serializable {
  private static final long serialVersionUID = 1058314411139470750L;

  private String mCommand = "";
  private JobConfInfo mConf = new JobConfInfo();

  /**
   * Creates a new instance of {@link CommandLineJobInfo}.
   */
  public CommandLineJobInfo() {}

  /**
   * Creates a new instance of {@link CommandLineJobInfo} from a thrift representation.
   *
   * @param commandLineJobInfo the thrift representation of a lineage command-line job information
   */
  protected CommandLineJobInfo(alluxio.thrift.CommandLineJobInfo commandLineJobInfo) {
    mCommand = commandLineJobInfo.getCommand();
    mConf = new JobConfInfo(commandLineJobInfo.getConf());
  }

  /**
   * @return the command
   */
  public String getCommand() {
    return mCommand;
  }

  /**
   * @return the command configuration
   */
  public JobConfInfo getConf() {
    return mConf;
  }

  /**
   * @param command the command to use
   * @return the lineage command-line job information
   */
  public CommandLineJobInfo setCommand(String command) {
    Preconditions.checkNotNull(command, "command");
    mCommand = command;
    return this;
  }

  /**
   * @param conf the command configuration to use
   * @return the lineage command-line job information
   */
  public CommandLineJobInfo setConf(JobConfInfo conf) {
    Preconditions.checkNotNull(conf);
    mConf = conf;
    return this;
  }

  /**
   * @return thrift representation of the lineage command-line job information
   */
  protected alluxio.thrift.CommandLineJobInfo toThrift() {
    return new alluxio.thrift.CommandLineJobInfo(mCommand, mConf.toThrift());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandLineJobInfo)) {
      return false;
    }
    CommandLineJobInfo that = (CommandLineJobInfo) o;
    return mCommand.equals(that.mCommand) && mConf.equals(that.mConf);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mCommand, mConf);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("command", mCommand).add("conf", mConf).toString();
  }
}
