package alluxio.master;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * A primary selector which allows the user to set the states manually.
 *
 * After creation, you must set the initial state to your desired initial state with
 * {@link #setState(State)}.
 */
public class ControllablePrimarySelector extends AbstractPrimarySelector {
  @Override
  public void start(InetSocketAddress localAddress) throws IOException {
      // nothing to do
  }

  @Override
  public void stop() throws IOException {
    // nothing to do
  }
}
