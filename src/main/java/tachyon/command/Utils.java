package tachyon.command;

import java.net.InetSocketAddress;
import tachyon.conf.MasterConf;

/**
 * Class for convenience methods used by TFsShell.
 */
public class Utils {
  private static final String HEADER = "tachyon://";

  /**
   * Validates the path, verifying that it contains the header and a hostname:port specified.
   * @param path The path to be verified.
   */
  public static String validateTachyonPath(String path) {
    if (!path.startsWith(HEADER)) {
      if (!path.contains(":")) {
    	if (path.startsWith("/")) {
          path = MasterConf.get().HOSTNAME + ":" + MasterConf.get().PORT + path;
    	} else {
    	  path = MasterConf.get().HOSTNAME + ":" + MasterConf.get().PORT + "/" + path;
    	}
      }
      path = HEADER + path;
    } else {
      String tempPath = path.substring(HEADER.length());
      if (!tempPath.contains(":")) {
        if (tempPath.startsWith("/")) {
          tempPath = MasterConf.get().HOSTNAME + ":" + MasterConf.get().PORT + tempPath;
        } else {
          tempPath = MasterConf.get().HOSTNAME + ":" + MasterConf.get().PORT + "/" + tempPath;
        }
      }
      path = HEADER + tempPath;
    }
    if (!path.endsWith("/")) {
      path = path + "/";
    }
    return path;
  }

  /**
   * Removes header and hostname:port information from a path, leaving only the local file path.
   * @param path The path to obtain the local path from
   * @return The local path in string format
   */ 
  public static String getFilePath(String path) {
    path = validateTachyonPath(path);
    path = path.substring(HEADER.length());
    String ret = path.substring(path.indexOf("/"));
    while (ret.endsWith("/")) {
      ret = ret.substring(0, ret.length()-1);
    }
    return ret;
  }

  /**
   * Obtains the InetSocketAddress from a path by parsing the hostname:port portion of the path.
   * @param path The path to obtain the InetSocketAddress from.
   * @return The InetSocketAddress of the master node.
   */
  public static InetSocketAddress getTachyonMasterAddress(String path) {
    path = validateTachyonPath(path);
    path = path.substring(HEADER.length());
    String masterAddress = path.substring(0, path.indexOf("/"));
    String masterHost = masterAddress.split(":")[0];
    int masterPort = Integer.parseInt(masterAddress.split(":")[1]);
    return new InetSocketAddress(masterHost, masterPort);
  }
}