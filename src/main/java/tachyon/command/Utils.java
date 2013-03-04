package tachyon.command;

import java.net.InetSocketAddress;

public class Utils {
  private static final String HEADER = "tachyon://";

  public static void validateTachyonPath(String path) {
    if (!path.startsWith(HEADER)) {
      System.out.println("The path format is " + HEADER + "<MASTERNODE>:<PORT>/<PATH_NAME>");
      System.exit(-1);
    }
    path = path.substring(HEADER.length());
    if (!path.contains(":") || !path.contains("/")) {
      System.out.println("The path format is " + HEADER + "<MASTERNODE>:<PORT>/<PATH_NAME>");
      System.exit(-1);
    }
  }

  public static String getFilePath(String path) {
    validateTachyonPath(path);
    path = path.substring(HEADER.length());
    return path.substring(path.indexOf("/"));
  }

  public static InetSocketAddress getTachyonMasterAddress(String path) {
    validateTachyonPath(path);
    path = path.substring(HEADER.length());
    String masterAddress = path.substring(0, path.indexOf("/"));
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new InetSocketAddress(masterHost, masterPort);
  }
}