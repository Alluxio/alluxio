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

package alluxio.cli.fsadmin.report;

import alluxio.conf.AlluxioConfiguration;
import alluxio.membership.MembershipManager;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;

/**
 * Command to get node status.
 */
public class NodeStatusCommand {

  private AlluxioConfiguration mConf;
  private PrintStream mPrintStream;

  /**
   * CTOR for NodeStatusCommand.
   * @param conf
   * @param printStream
   */
  public NodeStatusCommand(AlluxioConfiguration conf, PrintStream printStream) {
    mConf = conf;
    mPrintStream = printStream;
  }

  /**
   * Runs a proxy report command.
   * @param cl
   * @return 0 on success, 1 otherwise
   */
  public int run(CommandLine cl) throws IOException {
    MembershipManager memberMgr = MembershipManager.Factory.create(mConf);
    mPrintStream.println(memberMgr.showAllMembers());
    return 0;
  }
}
