package alluxio.logserver;

import alluxio.ProcessUtils;

public final class AlluxioLogServer {
  public static void main(String[] args) throws Exception {
    AlluxioLogServerProcess process = new AlluxioLogServerProcess(args[0], args[1]);
    ProcessUtils.run(process);
  }

  private AlluxioLogServer() {} // prevent instantiation
}
