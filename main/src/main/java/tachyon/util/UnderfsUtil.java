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
   * Load files under path "underfsAddress/rootPath" (excluding excludePathPrefix)
   * to the given tfs under its root directory "/".
   *
   * @param tfs
   *          the TFS handler created out of address like "tachyon://host:port"
   * @param underfsAddress
   *          the address of underFS server, like "hdfs://h:p", or "/" for local FS.
   * @param rootPath
   *          the source path in underFS, like "/dir".
   * @param excludePathPrefix
   *          paths to exclude from rootPath, which will not be registered in TFS.
   * @throws IOException
   */
  public static void loadUnderFs(TachyonFS tfs, String underfsAddress, String rootPath,
      PrefixList excludePathPrefix) throws IOException {
    getInfo(tfs, "/", underfsAddress, rootPath, excludePathPrefix);
  }

  /**
   * Load files under path "underfsAddress/rootPath" (excluding excludePathPrefix)
   * to the given tfs under the given tfsRoot directory.
   * 
   * @param tfs
   *          the TFS handler created out of address like "tachyon://host:port"
   * @param tfsRoot
   *          the destination point in TFS to load the under FS path onto
   * @param underfsAddress
   *          the address of underFS server, like "hdfs://h:p", or "" for local FS.
   * @param rootPath
   *          the source path in underFS, like "/dir".
   * @param excludePathPrefix
   *          paths to exclude from rootPath, which will not be registered in TFS.
   * @throws IOException
   */
  public static void getInfo(TachyonFS tfs, String tfsRoot, String underfsAddress,
      String rootPath, PrefixList excludePathPrefix) throws IOException {
    String underfsRootPath = (underfsAddress + rootPath).replace("//", "/");
    LOG.info(tfs + tfsRoot + " " + underfsRootPath + " " + excludePathPrefix);

    if (!tfs.exist(tfsRoot)) {
      tfs.mkdir(tfsRoot);
      LOG.info("directory " + tfsRoot + " does not exist in Tachyon: created");
    }

    Configuration tConf = new Configuration();
    tConf.set("fs.default.name", underfsRootPath);
    // TODO Use underfs to make this generic.
    UnderFileSystem fs = UnderFileSystem.get(underfsAddress);

    Queue<String> pathQueue = new LinkedList<String>();
    if (excludePathPrefix.outList(rootPath)) {
      pathQueue.add(underfsRootPath);
    }

    // only exclude prefix-matching files at the first level of given rootPath
    boolean isFirstLevel = true;

    while (!pathQueue.isEmpty()) {
      String path = pathQueue.poll();  // the absolute path
      if (fs.isFile(path)) {
        String tfsPath = createTFSPath(tfsRoot, underfsRootPath, path);
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
        String[] files = fs.list(path); // fs.list() returns paths relative to path
        if (files != null) {
          for (String filePath : files) {
            LOG.info("Get: " + filePath);
            String aPath = (path + "/" + filePath).replace("//", "/");
            if (isFirstLevel && excludePathPrefix.inList(filePath)) {
              LOG.info("excluded: "+filePath);
            } else {
              pathQueue.add(aPath);
            }
          }
          isFirstLevel = false;
        }
        String tfsPath = createTFSPath(tfsRoot, underfsRootPath, path);
        if (!tfs.exist(tfsPath)) {
          tfs.mkdir(tfsPath);
        }
      }
    }
  }

  /**
   * Create a new path relative to a given TFS root.
   *
   * @param tfsRoot
   *          the destination point in TFS to load the under FS path onto
   * @param ufsRootPath
   *          the source path in the under FS to be loaded
   * @param path
   *          the path relative to ufsRootPath of a file to be loaded
   * @return the new path relative to tfsRoot.
   */
  private static String createTFSPath(String tfsRoot, String ufsRootPath, String path) {
    String filePath = path.substring(ufsRootPath.length());
    if (filePath.isEmpty()) {
      // retrieve the basename in ufsRootPath
      filePath = path.substring(ufsRootPath.lastIndexOf("/") + 1);
    }
    return (tfsRoot + "/" + filePath).replace("//", "/");
  }

  public static void printUsage() {
    String cmd =
        "java -cp target/tachyon-" + Version.VERSION + "-jar-with-dependencies.jar "
            + "tachyon.util.UnderfsUtil ";
    // cmd = "bin/tachyon loadufs ";

    System.out.println("Usage: " + cmd + "<TachyonPath> <UnderfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out
        .println("Example: " + cmd + "tachyon://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a /b c");
    System.out.print("In the TFS, all files under local FS /b will be registered under /a, ");
    System.out.println("except for those with prefix c");
  }

  public static void main(String[] args) throws SuspectedFileSizeException, InvalidPathException,
      IOException, FileDoesNotExistException, FileAlreadyExistException, TException {

    if (!(args.length == 2 || args.length == 3)) {
      printUsage();
      System.exit(-1);
    }

    PrefixList tExcludePathPrefix = null;
    if (args.length == 3) {
      tExcludePathPrefix = new PrefixList(args[2], ";");
    } else {
      tExcludePathPrefix = new PrefixList(null);
    }

    // parse the given TachyonPath into a prefixing TFS address and a rootPath
    Pair<String, String> tfsPair = UnderFileSystem.parse(args[0]);

    // parse the given UnderfsPath into a prefixing UnderfsAddress and a rootPath
    Pair<String, String> ufsPair = UnderFileSystem.parse(args[1]);

    if (tfsPair == null || ufsPair == null) {
      printUsage();
      System.exit(-2);
    }

    getInfo(TachyonFS.get(tfsPair.getFirst()), tfsPair.getSecond(), ufsPair.getFirst(),
        ufsPair.getSecond(), tExcludePathPrefix);
    System.exit(0);
  }
}
