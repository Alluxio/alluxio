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

package alluxio.wire;

import alluxio.annotation.PublicApi;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * The lineage command-line job descriptor.
 */
@NotThreadSafe
@PublicApi
// TODO(jiri): Consolidate with alluxio.job.CommandLine.Job.
public final class CommandLineJobInfo {
  private String mCommand = "";
  private JobConfInfo mConf = new JobConfInfo();

  /**
   * Creates a new instance of {@link CommandLineJobInfo}.
   */
  public CommandLineJobInfo() {}

  /**
   * Creates a new instance of {@link CommandLineJobInfo} from a thrift representation.
   *
   * @param commandLineJobInfo the thrift representation of a lineage command-line job descriptor
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
   * @return the lineage command-line job descriptor
   */
  public CommandLineJobInfo setCommand(String command) {
    Preconditions.checkNotNull(command);
    mCommand = command;
    return this;
  }

  /**
   * @param conf the command configuration to use
   * @return the lineage command-line job descriptor
   */
  public CommandLineJobInfo setConf(JobConfInfo conf) {
    Preconditions.checkNotNull(conf);
    mConf = conf;
    return this;
  }

  /**
   * @return thrift representation of the lineage command-line job descriptor
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
