package tachyon.util;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

import tachyon.*;
import tachyon.client.TachyonFS;

/**
 * Utilities related to under filesystem
 */
public class UfsUtils {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /**
   * Build a new path relative to a given mTachyonFS root by retrieving the given path relative to
   * the ufsRootPath.
   * 
   * @param tfsRootPath
   *          the destination point in mTachyonFS to load the under FS path onto
   * @param ufsRootPath
   *          the source path in the under FS to be loaded
   * @param path
   *          the path in the under FS be loaded, path.startsWith(ufsRootPath) must be true
   * @return the new path relative to tfsRootPath.
   */
  @Deprecated
  private static String buildTFSPath(String tfsRootPath, String ufsRootPath, String path) {
    String filePath = path.substring(ufsRootPath.length());
    if (filePath.isEmpty()) {
      // retrieve the basename in ufsRootPath
      filePath = path.substring(ufsRootPath.lastIndexOf(TachyonURI.SEPARATOR) + 1);
    }
    return CommonUtils.concat(tfsRootPath, filePath);
  }

  /**
   * Build a new path relative to a given mTachyonFS root by retrieving the given path relative to
   * the ufsRootPath.
   *
   * @param tfsRootPath
   *          the destination point in mTachyonFS to load the under FS path onto
   * @param ufsRootPath
   *          the source path in the under FS to be loaded
   * @param path
   *          the path in the under FS be loaded, path.startsWith(ufsRootPath) must be true
   * @return the new path relative to tfsRootPath.
   */
  private static TachyonURI buildTFSPath(
      TachyonURI tfsRootPath, TachyonURI ufsRootPath, TachyonURI path) {
    String filePath = path.getPath().substring(ufsRootPath.getPath().length());
    if (filePath.isEmpty()) {
      // retrieve the basename in ufsRootPath
      filePath = path.getPath().substring(
          ufsRootPath.getPath().lastIndexOf(TachyonURI.SEPARATOR) + 1);
    }
    return new TachyonURI(CommonUtils.concat(tfsRootPath, filePath));
  }

  /**
   * Load files under path "ufsAddrRootPath" (excluding excludePathPrefix relative to the path)
   * to the given tfs under a given destination path.
   * 
   * @param tfsAddrRootPath
   *          the mTachyonFS address and path to load the src files, like "tachyon://host:port/dest".
   * @param ufsAddrRootPath
   *          the address and root path of the under FS, like "hdfs://host:port/src".
   * @param excludePaths
   *          paths to exclude from ufsRootPath, which will not be loaded in mTachyonFS.
   * @throws IOException
   */
  @Deprecated
  public static void loadUfs(String tfsAddrRootPath, String ufsAddrRootPath, String excludePaths)
      throws IOException {
    Pair<String, String> tfsPair = UnderFileSystem.parse(tfsAddrRootPath);
    String tfsAddress = tfsPair.getFirst();
    String tfsRootPath = tfsPair.getSecond();
    TachyonFS tfs = TachyonFS.get(tfsAddress);

    PrefixList excludePathPrefix = new PrefixList(excludePaths, ";");

    loadUnderFs(tfs, tfsRootPath, ufsAddrRootPath, excludePathPrefix);
  }

  /**
   * Load files under path "ufsAddrRootPath" (excluding excludePathPrefix relative to the path)
   * to the given tfs under a given destination path.
   *
   * @param tfsAddrRootPath
   *          the mTachyonFS address and path to load the src files, like "tachyon://host:port/dest".
   * @param ufsAddrRootPath
   *          the address and root path of the under FS, like "hdfs://host:port/src".
   * @param excludePaths
   *          paths to exclude from ufsRootPath, which will not be loaded in mTachyonFS.
   * @throws IOException
   */
  public static void loadUfs(TachyonURI tfsAddrRootPath, TachyonURI ufsAddrRootPath, String excludePaths)
      throws IOException {
    TachyonFS tfs = TachyonFS.get(tfsAddrRootPath);

    PrefixList excludePathPrefix = new PrefixList(excludePaths, ";");

    loadUnderFs(tfs, tfsAddrRootPath, ufsAddrRootPath, excludePathPrefix);
  }

  /**
   * Load files under path "ufsAddress/ufsRootPath" (excluding excludePathPrefix) to the given tfs
   * under the given tfsRootPath directory.
   *
   * @param tfs the mTachyonFS handler created out of address like "tachyon://host:port"
   * @param tfsRootPath the destination point in mTachyonFS to load the under FS path onto
   * @param ufsAddrRootPath the address and root path of the under FS, like "hdfs://host:port/dir".
   * @param excludePathPrefix paths to exclude from ufsRootPath, which will not be registered in
   *        mTachyonFS.
   * @throws IOException
   */
  @Deprecated
  public static void loadUnderFs(TachyonFS tfs, String tfsRootPath, String ufsAddrRootPath,
                                 PrefixList excludePathPrefix) throws IOException {
    LOG.info(tfs + tfsRootPath + " " + ufsAddrRootPath + " " + excludePathPrefix);

    try {
      // resolve and replace hostname embedded in the given ufsAddress
      String oldpath = ufsAddrRootPath;
      ufsAddrRootPath = NetworkUtils.replaceHostName(ufsAddrRootPath);
      if (!ufsAddrRootPath.equalsIgnoreCase(oldpath)) {
        System.out.println("UnderFS hostname resolved: " + ufsAddrRootPath);
      }
    } catch (UnknownHostException e) {
      LOG.info("hostname cannot be resolved in given UFS path: " + ufsAddrRootPath);
      throw new IOException(e);
    }

    Pair<String, String> ufsPair = UnderFileSystem.parse(ufsAddrRootPath);
    String ufsAddress = ufsPair.getFirst();
    String ufsRootPath = ufsPair.getSecond();

    if (!tfs.exist(tfsRootPath)) {
      tfs.mkdir(tfsRootPath);
      // TODO Add the following.
      // if (tfs.mkdir(tfsRootPath)) {
      // LOG.info("directory " + tfsRootPath + " does not exist in Tachyon: created");
      // } else {
      // throw new IOException("Failed to create folder in Tachyon: " + tfsRootPath);
      // }
    }

    // create the under FS handler (e.g. hdfs, local FS, s3 etc.)
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress);

    Queue<String> ufsPathQueue = new LinkedList<String>();
    if (excludePathPrefix.outList(ufsRootPath)) {
      ufsPathQueue.add(ufsAddrRootPath);
    }

    while (!ufsPathQueue.isEmpty()) {
      String ufsPath = ufsPathQueue.poll(); // this is the absolute path
      LOG.info("loading: " + ufsPath);
      if (ufs.isFile(ufsPath)) {
        String tfsPath = buildTFSPath(tfsRootPath, ufsAddrRootPath, ufsPath);
        if (tfs.exist(tfsPath)) {
          LOG.info("File " + tfsPath + " already exists in Tachyon.");
          continue;
        }
        int fileId = tfs.createFile(tfsPath, ufsPath);
        if (fileId == -1) {
          LOG.info("Failed to create tachyon file: " + tfsPath);
        } else {
          LOG.info("Create tachyon file " + tfsPath + " with file id " + fileId + " and "
              + "checkpoint location " + ufsPath);
        }
      } else { // ufsPath is a directory
        String[] files = ufs.list(ufsPath); // ufs.list() returns relative path
        if (files != null) {
          for (String filePath : files) {
            LOG.info("Get: " + filePath);
            String aPath = CommonUtils.concat(ufsPath, filePath);
            String checkPath = aPath.substring(ufsAddrRootPath.length());
            if (checkPath.startsWith(Constants.PATH_SEPARATOR)) {
              checkPath = checkPath.substring(Constants.PATH_SEPARATOR.length());
            }
            if (excludePathPrefix.inList(checkPath)) {
              LOG.info("excluded: " + checkPath);
            } else {
              ufsPathQueue.add(aPath);
            }
          }
        }
        // ufsPath is a directory, so only concat the tfsRoot with the relative path
        String tfsPath =
            CommonUtils.concat(tfsRootPath, ufsPath.substring(ufsAddrRootPath.length()));
        if (!tfs.exist(tfsPath)) {
          tfs.mkdir(tfsPath);
          // TODO Add the following.
          // if (tfs.mkdir(tfsPath)) {
          // LOG.info("Created mTachyonFS folder " + tfsPath + " with checkpoint location " +
          // ufsPath);
          // } else {
          // LOG.info("Failed to create tachyon folder: " + tfsPath);
          // }
        }
      }
    }
  }

  /**
   * Load files under path "ufsAddress/ufsRootPath" (excluding excludePathPrefix)
   * to the given tfs under the given tachyonPath directory.
   * 
   * @param tfs
   *          the mTachyonFS handler created out of address like "tachyon://host:port"
   * @param tachyonPath
   *          the path to load the under FS path onto, like "tachyon://host:port/dir" or "/dir"
   * @param ufsAddrRootPath
   *          the address and root path of the under FS, like "hdfs://host:port/dir".
   * @param excludePathPrefix
   *          paths to exclude from ufsRootPath, which will not be registered in mTachyonFS.
   * @throws IOException
   */
  public static void loadUnderFs(TachyonFS tfs, TachyonURI tachyonPath, TachyonURI ufsAddrRootPath,
      PrefixList excludePathPrefix) throws IOException {
    LOG.info("Loading to " + tachyonPath + " " + ufsAddrRootPath + " " + excludePathPrefix);

    try {
      // resolve and replace hostname embedded in the given ufsAddress
      TachyonURI oldPath = ufsAddrRootPath;
      ufsAddrRootPath = NetworkUtils.replaceHostName(ufsAddrRootPath);
      if (!ufsAddrRootPath.getHost().equalsIgnoreCase(oldPath.getHost())) {
        System.out.println("UnderFS hostname resolved: " + ufsAddrRootPath);
      }
    } catch (UnknownHostException e) {
      LOG.info("hostname cannot be resolved in given UFS path: " + ufsAddrRootPath);
      throw new IOException(e);
    }

    Pair<String, String> ufsPair = UnderFileSystem.parse(ufsAddrRootPath);
    String ufsAddress = ufsPair.getFirst();
    String ufsRootPath = ufsPair.getSecond();

    if (!tfs.exist(tachyonPath)) {
      tfs.mkdir(tachyonPath);
      // TODO Add the following.
      // if (tfs.mkdir(tfsRootPath)) {
      // LOG.info("directory " + tfsRootPath + " does not exist in Tachyon: created");
      // } else {
      // throw new IOException("Failed to create folder in Tachyon: " + tfsRootPath);
      // }
    }

    // create the under FS handler (e.g. hdfs, local FS, s3 etc.)
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress);

    Queue<TachyonURI> ufsPathQueue = new LinkedList<TachyonURI>();
    if (excludePathPrefix.outList(ufsRootPath)) {
      ufsPathQueue.add(ufsAddrRootPath);
    }

    while (!ufsPathQueue.isEmpty()) {
      TachyonURI ufsPath = ufsPathQueue.poll();  // this is the absolute path
      LOG.info("Loading: " + ufsPath);
      if (ufs.isFile(ufsPath.toString())) {
        TachyonURI tfsPath = buildTFSPath(tachyonPath, ufsAddrRootPath, ufsPath);
        if (tfs.exist(tfsPath)) {
          LOG.info("File " + tfsPath + " already exists in Tachyon.");
          continue;
        }
        int fileId = tfs.createFile(tfsPath, ufsPath);
        if (fileId == -1) {
          LOG.info("Failed to create tachyon file: " + tfsPath);
        } else {
          LOG.info("Create tachyon file " + tfsPath + " with file id " + fileId + " and "
              + "checkpoint location " + ufsPath);
        }
      } else { // ufsPath is a directory
        String[] files = ufs.list(ufsPath.toString()); // ufs.list() returns relative path
        if (files != null) {
          for (String filePath : files) {
            LOG.info("Get: " + filePath);
            String aPath = CommonUtils.concat(ufsPath, filePath);
            String checkPath = aPath.substring(ufsAddrRootPath.toString().length());
            if (checkPath.startsWith(TachyonURI.SEPARATOR)) {
              checkPath = checkPath.substring(TachyonURI.SEPARATOR.length());
            }
            if (excludePathPrefix.inList(checkPath)) {
              LOG.info("excluded: " + checkPath);
            } else {
              ufsPathQueue.add(new TachyonURI(aPath));
            }
          }
        }
        // ufsPath is a directory, so only concat the tfsRoot with the relative path
        TachyonURI tfsPath = new TachyonURI(CommonUtils.concat(
            tachyonPath, ufsPath.getPath().substring(ufsAddrRootPath.getPath().length())));
        if (!tfs.exist(tfsPath)) {
          tfs.mkdir(tfsPath);
          // TODO Add the following.
          // if (tfs.mkdir(tfsPath)) {
          // LOG.info("Created mTachyonFS folder " + tfsPath + " with checkpoint location " + ufsPath);
          // } else {
          // LOG.info("Failed to create tachyon folder: " + tfsPath);
          // }
        }
      }
    }
  }

  public static void main(String[] args) {
    if (!(args.length == 2 || args.length == 3)) {
      printUsage();
      System.exit(-1);
    }

    String exList = (args.length == 3) ? args[2] : "";

    try {
      loadUfs(new TachyonURI(args[0]), new TachyonURI(args[1]), exList);
    } catch (Exception e) {
      e.printStackTrace();
      printUsage();
      System.exit(-1);
    }

    System.exit(0);
  }

  public static void printUsage() {
    String cmd =
        "java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
            + "tachyon.util.UfsUtils ";

    System.out.println("Usage: " + cmd + "<TachyonPath> <UfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out
        .println("Example: " + cmd + "tachyon://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a /b c");
    System.out.print("In the TFS, all files under local FS /b will be registered under /a, ");
    System.out.println("except for those with prefix c");
  }
}
