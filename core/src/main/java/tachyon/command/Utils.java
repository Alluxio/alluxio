package tachyon.command;

import java.io.IOException;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.conf.CommonConf;
import tachyon.util.CommonUtils;

/**
 * Class for convenience methods used by TFsShell.
 */
public class Utils {
  /**
   * Removes Constants.HEADER / Constants.HEADER_FT and hostname:port information from a path,
   * leaving only the local file path.
   * 
   * @param path The path to obtain the local path from
   * @return The local path in string format
   * @throws IOException
   */
  public static String getFilePath(String path) throws IOException {
    path = validatePath(path);
    if (path.startsWith(Constants.HEADER)) {
      path = path.substring(Constants.HEADER.length());
    } else if (path.startsWith(Constants.HEADER_FT)) {
      path = path.substring(Constants.HEADER_FT.length());
    }
    String ret = path.substring(path.indexOf(TachyonURI.SEPARATOR));
    return ret;
  }

  /**
   * Validates the path, verifying that it contains the <code>Constants.HEADER </code> or
   * <code>Constants.HEADER_FT</code> and a hostname:port specified.
   * 
   * @param path The path to be verified.
   * @return the verified path in a form like tachyon://host:port/dir. If only the "/dir" or "dir"
   *         part is provided, the host and port are retrieved from property,
   *         tachyon.master.hostname and tachyon.master.port, respectively.
   * @throws IOException if the given path is not valid.
   */
  public static String validatePath(String path) throws IOException {
    if (path.startsWith(Constants.HEADER) || path.startsWith(Constants.HEADER_FT)) {
      if (!path.contains(":")) {
        throw new IOException("Invalid Path: " + path + ". Use " + Constants.HEADER
            + "host:port/ ," + Constants.HEADER_FT + "host:port/" + " , or /file");
      } else {
        return path;
      }
    } else {
      String HOSTNAME = System.getProperty("tachyon.master.hostname", "localhost");
      String PORT = System.getProperty("tachyon.master.port", "" + Constants.DEFAULT_MASTER_PORT);
      if (CommonConf.get().USE_ZOOKEEPER) {
        return CommonUtils.concat(Constants.HEADER_FT + HOSTNAME + ":" + PORT, path);
      }
      return CommonUtils.concat(Constants.HEADER + HOSTNAME + ":" + PORT, path);
    }
  }
}
