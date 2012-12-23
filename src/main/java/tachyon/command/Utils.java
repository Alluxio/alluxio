package tachyon.command;

import java.net.InetSocketAddress;

public class Utils {
  public static void validateTachyonPath(String path) {
    if (!path.startsWith("tachyon://")) {
      System.out.println("The path name has to start with tachyon://");
      System.exit(-1);
    }
    if (!path.contains(":") || !path.contains("/")) {
      System.out.println("The path name is invalid.");
      System.exit(-1);
    }
  }
  
  public static String getDatasetName(String path) {
    validateTachyonPath(path);
    path = path.substring(10);
    return path.substring(path.indexOf("/"));
  }

  public static InetSocketAddress getTachyonMasterAddress(String path) {
    validateTachyonPath(path);
    path = path.substring(10);
    String masterAddress = path.substring(0, path.indexOf("/"));
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new InetSocketAddress(masterHost, masterPort);
  }
}