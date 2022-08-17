package alluxio.master.journal.ufs;

import alluxio.master.AbstractPrimarySelector;

import java.io.IOException;
import java.net.InetSocketAddress;

public class UFSJournalSingleMasterPrimarySelector extends AbstractPrimarySelector {
  @Override
  public void start(InetSocketAddress localAddress) throws IOException {
    setState(State.PRIMARY);
  }

  @Override
  public void stop() throws IOException {
    // do nothing
  }
}
