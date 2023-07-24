package alluxio.cli.fsadmin.report;

import alluxio.conf.AlluxioConfiguration;
import alluxio.membership.MembershipManager;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;

public class NodeStatusCommand {

  private AlluxioConfiguration mConf;
  private PrintStream mPrintStream;

  public NodeStatusCommand(AlluxioConfiguration conf, PrintStream printStream) {
    mConf = conf;
    mPrintStream = printStream;
  }

  /**
   * Runs a proxy report command.
   *
   * @return 0 on success, 1 otherwise
   */
  public int run(CommandLine cl) throws IOException {
    MembershipManager memberMgr = MembershipManager.Factory.create(mConf);
    mPrintStream.println(memberMgr.showAllMembers());
    return 0;
  }
}
