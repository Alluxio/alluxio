/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.util;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.PrefixList;
import tachyon.UnderFileSystem;
import tachyon.Version;
import tachyon.client.TachyonFS;

/**
 * Utilities related to under filesystem
 */
public class UnderfsUtils {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

  /**
   * Build a new path relative to a given TFS root by retrieving the given path relative to
   * the ufsRootPath.
   *
   * @param tfsRootPath
   *          the destination point in TFS to load the under FS path onto
   * @param ufsRootPath
   *          the source path in the under FS to be loaded
   * @param path
   *          the path in the under FS be loaded, path.startsWith(ufsRootPath) must be true
   * @return the new path relative to tfsRootPath.
   */
  private static String buildTFSPath(String tfsRootPath, String ufsRootPath, String path) {
    String filePath = path.substring(ufsRootPath.length());
    if (filePath.isEmpty()) {
      // retrieve the basename in ufsRootPath
      filePath = path.substring(ufsRootPath.lastIndexOf(Constants.PATH_SEPARATOR) + 1);
    }
    return CommonUtils.concat(tfsRootPath, filePath);
  }

  /**
   * Load files under path "ufsAddrRootPath" (excluding excludePathPrefix relative to the path)
   * to the given tfs under a given destination path.
   *
   * @param tfsAddrRootPath
   *          the TFS address and path to load the src files, like "tachyon://host:port/dest".
   * @param ufsAddrRootPath
   *          the address and root path of the under FS, like "hdfs://host:port/src".
   * @param excludePaths
   *          paths to exclude from ufsRootPath, which will not be loaded in TFS.
   * @throws IOException
   */
  public static void loadUnderFs(String tfsAddrRootPath, String ufsAddrRootPath,
      String excludePaths) throws IOException {
    Pair<String, String> tfsPair = UnderFileSystem.parse(tfsAddrRootPath);
    String tfsAddress = tfsPair.getFirst();
    String tfsRootPath = tfsPair.getSecond();
    TachyonFS tfs = TachyonFS.get(tfsAddress);

    PrefixList excludePathPrefix = new PrefixList(excludePaths, ";");

    loadUnderFs(tfs, tfsRootPath, ufsAddrRootPath, excludePathPrefix);
  }

  /**
   * Load files under path "ufsAddress/ufsRootPath" (excluding excludePathPrefix)
   * to the given tfs under the given tfsRootPath directory.
   *
   * @param tfs
   *          the TFS handler created out of address like "tachyon://host:port"
   * @param tfsRootPath
   *          the destination point in TFS to load the under FS path onto
   * @param ufsAddrRootPath
   *          the address and root path of the under FS, like "hdfs://host:port/dir".
   * @param excludePathPrefix
   *          paths to exclude from ufsRootPath, which will not be registered in TFS.
   * @throws IOException
   */
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
      String ufsPath = ufsPathQueue.poll();  // this is the absolute path
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
          // LOG.info("Created TFS folder " + tfsPath + " with checkpoint location " + ufsPath);
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
      loadUnderFs(args[0], args[1], exList);
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
            + "tachyon.util.UnderfsUtils ";

    System.out.println("Usage: " + cmd + "<TachyonPath> <UnderfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out
        .println("Example: " + cmd + "tachyon://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a /b c");
    System.out.print("In the TFS, all files under local FS /b will be registered under /a, ");
    System.out.println("except for those with prefix c");
  }
}
