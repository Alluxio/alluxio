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
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import tachyon.Constants;
import tachyon.Pair;
import tachyon.PrefixList;
import tachyon.UnderFileSystem;
import tachyon.Version;
import tachyon.client.TachyonFS;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.InvalidPathException;
import tachyon.thrift.SuspectedFileSizeException;

/**
 * Utilities related to under filesystem
 */
public class UnderfsUtil {
  private static Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);

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

    Pair<String, String> tfsPair = UnderFileSystem.parse(ufsAddrRootPath);
    String ufsAddress = tfsPair.getFirst();
    String ufsRootPath = tfsPair.getSecond();

    if (!tfs.exist(tfsRootPath)) {
      tfs.mkdir(tfsRootPath);
      LOG.info("directory " + tfsRootPath + " does not exist in Tachyon: created");
    }

    // create the under FS handler (for hdfs, local FS, or s3)
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress);

    Queue<String> pathQueue = new LinkedList<String>();
    if (excludePathPrefix.outList(ufsRootPath)) {
      pathQueue.add(ufsAddrRootPath);
    }

    while (!pathQueue.isEmpty()) {
      String path = pathQueue.poll();  // the absolute path
      if (ufs.isFile(path)) {
        String tfsPath = createTFSPath(tfsRootPath, ufsAddrRootPath, path);
        if (tfs.exist(tfsPath)) {
          LOG.info("File " + tfsPath + " already exists in Tachyon.");
          continue;
        }
        int fileId = tfs.createFile(tfsPath, path);
        if (fileId == -1) {
          LOG.info("Failed to create tachyon file: " + tfsPath);
        } else {
          LOG.info("Create tachyon file " + tfsPath + " with file id " + fileId + " and "
              + "checkpoint location " + path);
        }
      } else { // path is a directory
        String[] files = ufs.list(path); // ufs.list() returns paths relative to path
        if (files != null) {
          for (String filePath : files) {
            LOG.info("Get: " + filePath);
            String aPath = CommonUtils.concat(path, filePath);
            String checkPath = aPath.substring(ufsAddrRootPath.length());
            if (checkPath.startsWith(Constants.PATH_SEPARATOR)) {
              checkPath = checkPath.substring(Constants.PATH_SEPARATOR.length());
            }
            if (excludePathPrefix.inList(checkPath)) {
              LOG.info("excluded: " + checkPath);
            } else {
              pathQueue.add(aPath);
            }
          }
        }
        String tfsPath = createTFSPath(tfsRootPath, ufsAddrRootPath, path);
        if (!tfs.exist(tfsPath)) {
          tfs.mkdir(tfsPath);
        }
      }
    }
  }

  /**
   * Create a new path relative to a given TFS root.
   *
   * @param tfsRootPath
   *          the destination point in TFS to load the under FS path onto
   * @param ufsRootPath
   *          the source path in the under FS to be loaded
   * @param path
   *          the path relative to ufsRootPath of a file to be loaded
   * @return the new path relative to tfsRootPath.
   */
  private static String createTFSPath(String tfsRootPath, String ufsRootPath, String path) {
    String filePath = path.substring(ufsRootPath.length());
    if (filePath.isEmpty()) {
      // retrieve the basename in ufsRootPath
      filePath = path.substring(ufsRootPath.lastIndexOf("/") + 1);
    }
    return CommonUtils.concat(tfsRootPath, filePath);
  }

  public static void printUsage() {
    String cmd =
        "java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
            + "tachyon.util.UnderfsUtil ";

    System.out.println("Usage: " + cmd + "<TachyonPath> <UnderfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out
        .println("Example: " + cmd + "tachyon://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a /b c");
    System.out.print("In the TFS, all files under local FS /b will be registered under /a, ");
    System.out.println("except for those with prefix c");
  }

  public static void main(String[] args) {

    if (!(args.length == 2 || args.length == 3)) {
      printUsage();
      System.exit(-1);
    }

    String exList = (args.length==3) ? args[2] : "";

    try {
      loadUnderFs(args[0], args[1], exList);
    } catch (Exception e) {
      e.printStackTrace();
      printUsage();
      System.exit(-2);
    }

    System.exit(0);
  }
}
