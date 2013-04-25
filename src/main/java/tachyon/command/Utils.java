package tachyon.command;

import java.net.InetSocketAddress;

import tachyon.Constants;
import tachyon.thrift.InvalidPathException;

/**
 * Class for convenience methods used by TFsShell.
 */
public class Utils {
  private static final String HEADER = "tachyon://";
  /**
   * Validates the path, verifying that it contains the header and a hostname:port specified.
   * @param path The path to be verified.
   * @throws InvalidPathException 
   */
  public static String validateTachyonPath(String path) throws InvalidPathException {
    if (path.startsWith(HEADER)) {
      if (!path.contains(":")) {
        throw new InvalidPathException(
            "Invalid Path: " + path + "\n Use tachyon://host:port/ or /file");
      } else {
        return path;
      }
    } else {
      String HOSTNAME = System.getProperty("tachyon.master.hostname", "localhost");
      String PORT = System.getProperty("tachyon.master.port", "" + Constants.DEFAULT_MASTER_PORT);
      return HEADER + HOSTNAME + ":" + PORT + path;
    }
  }

  /**
   * Removes header and hostname:port information from a path, leaving only the local file path.
   * @param path The path to obtain the local path from
   * @return The local path in string format
   * @throws InvalidPathException 
   */ 
  public static String getFilePath(String path) throws InvalidPathException {
    path = validateTachyonPath(path);
    path = path.substring(HEADER.length());
    String ret = path.substring(path.indexOf("/"));
    return ret;
  }

  /**
   * Obtains the InetSocketAddress from a path by parsing the hostname:port portion of the path.
   * @param path The path to obtain the InetSocketAddress from.
   * @return The InetSocketAddress of the master node.
   * @throws InvalidPathException 
   */
  public static InetSocketAddress getTachyonMasterAddress(String path) throws InvalidPathException {
    path = validateTachyonPath(path);
    path = path.substring(HEADER.length());
    String masterAddress = path.substring(0, path.indexOf("/"));
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new InetSocketAddress(masterHost, masterPort);
  }
}