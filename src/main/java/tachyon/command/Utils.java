package tachyon.command;

import java.net.InetSocketAddress;

/**
 * Class for convenience methods used by TFsShell.
 */
public class Utils {
  private static final String HEADER = "tachyon://";

  /**
   * Validates the path, verifying that it contains the header and a hostname:port specified.
   * @param path The path to be verified.
   */
  public static void validateTachyonPath(String path) {
    if (!path.startsWith(HEADER)) {
      System.out.println("The path format is " + HEADER + "<MASTERHOST>:<PORT>/<PATH_NAME>");
      System.exit(-1);
    }
    path = path.substring(HEADER.length());
    if (!path.contains(":") || !path.contains("/")) {
      System.out.println("The path format is " + HEADER + "<MASTERHOST>:<PORT>/<PATH_NAME>");
      System.exit(-1);
    }
  }

  /**
   * Removes header and hostname:port information from a path, leaving only the local file path.
   * @param path The path to obtain the local path from
   * @return The local path in string format
   */ 
  public static String getFilePath(String path) {
    validateTachyonPath(path);
    path = path.substring(HEADER.length());
    return path.substring(path.indexOf("/"));
  }

  /**
   * Obtains the InetSocketAddress from a path by parsing the hostname:port portion of the path.
   * @param path The path to obtain the InetSocketAddress from.
   * @return The InetSocketAddress of the master node.
   */
  public static InetSocketAddress getTachyonMasterAddress(String path) {
    validateTachyonPath(path);
    path = path.substring(HEADER.length());
    String masterAddress = path.substring(0, path.indexOf("/"));
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new InetSocketAddress(masterHost, masterPort);
  }
}