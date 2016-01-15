/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.Version;
import tachyon.client.file.FileSystem;
import tachyon.collections.Pair;
import tachyon.collections.PrefixList;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.underfs.UnderFileSystem;
import tachyon.util.io.PathUtils;
import tachyon.util.network.NetworkAddressUtils;

/**
 * Utilities related to under file system.
 */
public final class UfsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Builds a new path relative to a given mTachyonFS root by retrieving the given path relative to
   * the ufsRootPath.
   *
   * @param tfsRootPath the destination point in mTachyonFS to load the under FS path onto
   * @param ufsRootPath the source path in the under FS to be loaded
   * @param path the path in the under FS be loaded, path.startsWith(ufsRootPath) must be true
   * @return the new path relative to tfsRootPath
   */
  private static TachyonURI buildTFSPath(
      TachyonURI tfsRootPath, TachyonURI ufsRootPath, TachyonURI path) {
    String filePath = path.getPath().substring(ufsRootPath.getPath().length());
    if (filePath.isEmpty()) {
      // retrieve the basename in ufsRootPath
      filePath = path.getPath().substring(
          ufsRootPath.getPath().lastIndexOf(TachyonURI.SEPARATOR) + 1);
    }
    return new TachyonURI(PathUtils.concatPath(tfsRootPath, filePath));
  }

  /**
   * Loads files under path "ufsAddrRootPath" (excluding excludePathPrefix relative to the path) to
   * the given tfs under a given destination path.
   *
   * @param tfsAddrRootPath the mTachyonFS address and path to load the src files, like
   *        "tachyon://host:port/dest".
   * @param ufsAddrRootPath the address and root path of the under FS, like "hdfs://host:port/src"
   * @param excludePaths paths to exclude from ufsRootPath, which will not be loaded in mTachyonFS
   * @param tachyonConf the instance of {@link tachyon.conf.TachyonConf} to be used
   * @throws IOException when an event that prevents the operation from completing is encountered
   * @throws TachyonException if an unexpected tachyon error occurs
   */
  private static void loadUfs(TachyonURI tfsAddrRootPath, TachyonURI ufsAddrRootPath,
      String excludePaths, TachyonConf tachyonConf) throws IOException, TachyonException {
    FileSystem tfs = FileSystem.Factory.get();

    PrefixList excludePathPrefix = new PrefixList(excludePaths, ";");

    loadUfs(tfs, tfsAddrRootPath, ufsAddrRootPath, excludePathPrefix, tachyonConf);
  }

  /**
   * Loads files under path "ufsAddress/ufsRootPath" (excluding excludePathPrefix) to the given fs
   * under the given tfsRootPath directory.
   *
   * @param fs the {@link FileSystem} handler created out of address like "tachyon://host:port"
   * @param tachyonPath the destination point in mTachyonFS to load the under FS path onto
   * @param ufsAddrRootPath the address and root path of the under FS, like "hdfs://host:port/dir"
   * @param excludePathPrefix paths to exclude from ufsRootPath, which will not be registered in
   *        mTachyonFS.
   * @param tachyonConf instance of {@link TachyonConf}
   * @throws IOException when an event that prevents the operation from completing is encountered
   * @throws TachyonException if an unexpected tachyon error occurs
   * @deprecated As of version 0.8.
   *             Use {@link #loadUfs(TachyonURI, TachyonURI, String, TachyonConf)} instead.
   */
  @Deprecated
  public static void loadUfs(FileSystem fs, TachyonURI tachyonPath, TachyonURI
      ufsAddrRootPath, PrefixList excludePathPrefix, TachyonConf tachyonConf) throws IOException,
      TachyonException {
    LOG.info("Loading to {} {} {}", tachyonPath, ufsAddrRootPath, excludePathPrefix);
    try {
      // resolve and replace hostname embedded in the given ufsAddress/tachyonAddress
      ufsAddrRootPath = NetworkAddressUtils.replaceHostName(ufsAddrRootPath);
      tachyonPath = NetworkAddressUtils.replaceHostName(tachyonPath);
    } catch (UnknownHostException e) {
      LOG.error("Failed to resolve hostname", e);
      throw new IOException(e);
    }

    Pair<String, String> ufsPair = UnderFileSystem.parse(ufsAddrRootPath, tachyonConf);
    String ufsAddress = ufsPair.getFirst();
    String ufsRootPath = ufsPair.getSecond();

    LOG.debug("Loading ufs, address:{}; root path: {}", ufsAddress, ufsRootPath);

    // create the under FS handler (e.g. hdfs, local FS, s3 etc.)
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, tachyonConf);

    if (!ufs.exists(ufsAddrRootPath.toString())) {
      throw new FileNotFoundException("ufs path " + ufsAddrRootPath + " not found.");
    }

    // directory name to load, either the path parent or the actual path if it is a directory
    TachyonURI directoryName;
    if (ufs.isFile(ufsAddrRootPath.toString())) {
      if ((ufsRootPath == null) || ufsRootPath.isEmpty() || ufsRootPath.equals("/")) {
        directoryName = TachyonURI.EMPTY_URI;
      } else {
        int lastSlashPos = ufsRootPath.lastIndexOf('/');
        if (lastSlashPos > 0) {
          directoryName = new TachyonURI(ufsRootPath.substring(0, lastSlashPos)); // trim the slash
        } else {
          directoryName = TachyonURI.EMPTY_URI;
        }
      }
    } else {
      directoryName = tachyonPath;
    }

    if (!directoryName.equals(TachyonURI.EMPTY_URI)) {
      if (!fs.exists(directoryName)) {
        LOG.debug("Loading ufs. Make dir if needed for '{}'.", directoryName);
        fs.createDirectory(directoryName);
      }
      // TODO(hy): Add the following.
      // if (fs.mkdir(tfsRootPath)) {
      // LOG.info("directory " + tfsRootPath + " does not exist in Tachyon: created");
      // } else {
      // throw new IOException("Failed to create folder in Tachyon: " + tfsRootPath);
      // }
    }

    Queue<TachyonURI> ufsPathQueue = new LinkedList<TachyonURI>();
    if (excludePathPrefix.outList(ufsRootPath)) {
      ufsPathQueue.add(ufsAddrRootPath);
    }

    while (!ufsPathQueue.isEmpty()) {
      TachyonURI ufsPath = ufsPathQueue.poll(); // this is the absolute path
      LOG.debug("Loading: {}", ufsPath);
      if (ufs.isFile(ufsPath.toString())) { // TODO(hy): Fix path matching issue.
        TachyonURI tfsPath = buildTFSPath(directoryName, ufsAddrRootPath, ufsPath);
        LOG.debug("Loading ufs. fs path = {}.", tfsPath);
        if (fs.exists(tfsPath)) {
          LOG.debug("File {} already exists in Tachyon.", tfsPath);
          continue;
        } else {
          fs.loadMetadata(tfsPath);
        }
        LOG.debug("Create tachyon file {} with checkpoint location {}", tfsPath, ufsPath);
      } else { // ufsPath is a directory
        LOG.debug("Loading ufs. ufs path is a directory.");
        String[] files = ufs.list(ufsPath.toString()); // ufs.list() returns relative path
        if (files != null) {
          for (String filePath : files) {
            if (filePath.isEmpty()) { // Prevent infinite loops
              continue;
            }
            LOG.debug("Get: {}", filePath);
            String aPath = PathUtils.concatPath(ufsPath, filePath);
            String checkPath = aPath.substring(ufsAddrRootPath.toString().length());
            if (checkPath.startsWith(TachyonURI.SEPARATOR)) {
              checkPath = checkPath.substring(TachyonURI.SEPARATOR.length());
            }
            if (excludePathPrefix.inList(checkPath)) {
              LOG.debug("excluded: {}", checkPath);
            } else {
              ufsPathQueue.add(new TachyonURI(aPath));
            }
          }
        }
        // ufsPath is a directory, so only concat the tfsRoot with the relative path
        TachyonURI tfsPath = new TachyonURI(PathUtils.concatPath(
            tachyonPath, ufsPath.getPath().substring(ufsAddrRootPath.getPath().length())));
        LOG.debug("Loading ufs. ufs path is a directory. tfsPath = {}.", tfsPath);
        if (!fs.exists(tfsPath)) {
          LOG.debug("Loading ufs. ufs path is a directory. make dir = {}.", tfsPath);
          fs.loadMetadata(tfsPath);
          // TODO(hy): Add the following.
          // if (fs.mkdir(tfsPath)) {
          // LOG.info("Created mTachyonFS folder {} with checkpoint location {}", tfsPath, ufsPath);
          // } else {
          // LOG.info("Failed to create tachyon folder: {}", tfsPath);
          // }
        }
      }
    }
  }

  /**
   * Starts the command line utility to load files under path "ufsAddress/ufsRootPath"
   * (excluding excludePathPrefix) to the given tfs under the given tfsRootPath directory.
   *
   * @param args the parameters as <TachyonPath> <UfsPath> [<Optional ExcludePathPrefix, seperated
   *             by ;>]
   */
  public static void main(String[] args) {
    if (!(args.length == 2 || args.length == 3)) {
      printUsage();
      System.exit(-1);
    }

    String exList = (args.length == 3) ? args[2] : "";

    try {
      loadUfs(new TachyonURI(args[0]), new TachyonURI(args[1]), exList, new TachyonConf());
    } catch (Exception e) {
      e.printStackTrace();
      printUsage();
      System.exit(-1);
    }

    System.exit(0);
  }

  /**
   * Prints an example usage of the command line.
   */
  public static void printUsage() {
    String cmd = "java -cp " + Version.TACHYON_JAR + " tachyon.client.UfsUtils ";

    System.out.println("Usage: " + cmd + "<TachyonPath> <UfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "tachyon://127.0.0.1:19998/a /b c");
    System.out.print("In the TFS, all files under local FS /b will be registered under /a, ");
    System.out.println("except for those with prefix c");
  }

  private UfsUtils() {} // prevent instantiation
}
