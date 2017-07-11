package alluxio.cli.validation;

import java.util.List;

/**
 * Task for validating SSH reachability.
 */
public class SshValidationTask implements ValidationTask {
  private final String mFileName;

  /**
   * Creates a new instance of {@link SshValidationTask}.
   *
   * @param fileName file name for a node list file (e.g. conf/workers, conf/masters)
   */
  public SshValidationTask(String fileName) {
    mFileName = fileName;
  }

  @Override
  public boolean validate() {
    List<String> nodes = Utils.readNodeList(mFileName);
    if (nodes == null) {
      return false;
    }

    boolean hasUnreachableNodes = false;
    for (String nodeName : nodes) {
      if (!Utils.isAddressReachable(nodeName, 22)) {
        System.err.format("Unable to reach ssh port 22 on node %s.%n", nodeName);
        hasUnreachableNodes = true;
      }
    }
    return !hasUnreachableNodes;
  }
}
