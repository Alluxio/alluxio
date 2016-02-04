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

package alluxio.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Queue;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alluxio.Constants;
import alluxio.AlluxioURI;
import alluxio.Version;
import alluxio.client.file.FileSystem;
import alluxio.collections.Pair;
import alluxio.collections.PrefixList;
import alluxio.Configuration;
import alluxio.exception.AlluxioException;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.io.PathUtils;
import alluxio.util.network.NetworkAddressUtils;

/**
 * Utilities related to under file system.
 */
@ThreadSafe
public final class UfsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Builds a new path relative to a given root by retrieving the given path relative to
   * the ufsRootPath.
   *
   * @param tfsRootPath the destination point in mTachyonFS to load the under FS path onto
   * @param ufsRootPath the source path in the under FS to be loaded
   * @param path the path in the under FS be loaded, path.startsWith(ufsRootPath) must be true
   * @return the new path relative to tfsRootPath
   */
  private static AlluxioURI buildTFSPath(
      AlluxioURI tfsRootPath, AlluxioURI ufsRootPath, AlluxioURI path) {
    String filePath = path.getPath().substring(ufsRootPath.getPath().length());
    if (filePath.isEmpty()) {
      // retrieve the basename in ufsRootPath
      filePath = path.getPath().substring(
          ufsRootPath.getPath().lastIndexOf(AlluxioURI.SEPARATOR) + 1);
    }
    return new AlluxioURI(PathUtils.concatPath(tfsRootPath, filePath));
  }

  /**
   * Loads files under path "ufsAddrRootPath" (excluding excludePathPrefix relative to the path) to
   * the given tfs under a given destination path.
   *
   * @param tfsAddrRootPath the mTachyonFS address and path to load the src files, like
   *        "alluxio://host:port/dest".
   * @param ufsAddrRootPath the address and root path of the under FS, like "hdfs://host:port/src"
   * @param excludePaths paths to exclude from ufsRootPath, which will not be loaded in mTachyonFS
   * @param configuration the instance of {@link Configuration} to be used
   * @throws IOException when an event that prevents the operation from completing is encountered
   * @throws AlluxioException if an unexpected alluxio error occurs
   */
  private static void loadUfs(AlluxioURI tfsAddrRootPath, AlluxioURI ufsAddrRootPath,
                              String excludePaths, Configuration configuration) throws IOException, AlluxioException {
    FileSystem tfs = FileSystem.Factory.get();

    PrefixList excludePathPrefix = new PrefixList(excludePaths, ";");

    loadUfs(tfs, tfsAddrRootPath, ufsAddrRootPath, excludePathPrefix, configuration);
  }

  /**
   * Loads files under path "ufsAddress/ufsRootPath" (excluding excludePathPrefix) to the given fs
   * under the given tfsRootPath directory.
   *
   * @param fs the {@link FileSystem} handler created out of address like "alluxio://host:port"
   * @param tachyonPath the destination point in mTachyonFS to load the under FS path onto
   * @param ufsAddrRootPath the address and root path of the under FS, like "hdfs://host:port/dir"
   * @param excludePathPrefix paths to exclude from ufsRootPath, which will not be registered in
   *        mTachyonFS.
   * @param configuration instance of {@link Configuration}
   * @throws IOException when an event that prevents the operation from completing is encountered
   * @throws AlluxioException if an unexpected alluxio error occurs
   * @deprecated As of version 0.8.
   *             Use {@link #loadUfs(AlluxioURI, AlluxioURI, String, Configuration)} instead.
   */
  @Deprecated
  public static void loadUfs(FileSystem fs, AlluxioURI tachyonPath, AlluxioURI
      ufsAddrRootPath, PrefixList excludePathPrefix, Configuration configuration) throws IOException,
      AlluxioException {
    LOG.info("Loading to {} {} {}", tachyonPath, ufsAddrRootPath, excludePathPrefix);
    try {
      // resolve and replace hostname embedded in the given ufsAddress/tachyonAddress
      ufsAddrRootPath = NetworkAddressUtils.replaceHostName(ufsAddrRootPath);
      tachyonPath = NetworkAddressUtils.replaceHostName(tachyonPath);
    } catch (UnknownHostException e) {
      LOG.error("Failed to resolve hostname", e);
      throw new IOException(e);
    }

    Pair<String, String> ufsPair = UnderFileSystem.parse(ufsAddrRootPath, configuration);
    String ufsAddress = ufsPair.getFirst();
    String ufsRootPath = ufsPair.getSecond();

    LOG.debug("Loading ufs, address:{}; root path: {}", ufsAddress, ufsRootPath);

    // create the under FS handler (e.g. hdfs, local FS, s3 etc.)
    UnderFileSystem ufs = UnderFileSystem.get(ufsAddress, configuration);

    if (!ufs.exists(ufsAddrRootPath.toString())) {
      throw new FileNotFoundException("ufs path " + ufsAddrRootPath + " not found.");
    }

    // directory name to load, either the path parent or the actual path if it is a directory
    AlluxioURI directoryName;
    if (ufs.isFile(ufsAddrRootPath.toString())) {
      if ((ufsRootPath == null) || ufsRootPath.isEmpty() || ufsRootPath.equals("/")) {
        directoryName = AlluxioURI.EMPTY_URI;
      } else {
        int lastSlashPos = ufsRootPath.lastIndexOf('/');
        if (lastSlashPos > 0) {
          directoryName = new AlluxioURI(ufsRootPath.substring(0, lastSlashPos)); // trim the slash
        } else {
          directoryName = AlluxioURI.EMPTY_URI;
        }
      }
    } else {
      directoryName = tachyonPath;
    }

    if (!directoryName.equals(AlluxioURI.EMPTY_URI)) {
      if (!fs.exists(directoryName)) {
        LOG.debug("Loading ufs. Make dir if needed for '{}'.", directoryName);
        fs.createDirectory(directoryName);
      }
    }

    Queue<AlluxioURI> ufsPathQueue = new LinkedList<AlluxioURI>();
    if (excludePathPrefix.outList(ufsRootPath)) {
      ufsPathQueue.add(ufsAddrRootPath);
    }

    while (!ufsPathQueue.isEmpty()) {
      AlluxioURI ufsPath = ufsPathQueue.poll(); // this is the absolute path
      LOG.debug("Loading: {}", ufsPath);
      if (ufs.isFile(ufsPath.toString())) { // TODO(hy): Fix path matching issue.
        AlluxioURI tfsPath = buildTFSPath(directoryName, ufsAddrRootPath, ufsPath);
        LOG.debug("Loading ufs. fs path = {}.", tfsPath);
        if (fs.exists(tfsPath)) {
          LOG.debug("File {} already exists in Tachyon.", tfsPath);
          continue;
        } else {
          fs.loadMetadata(tfsPath);
        }
        LOG.debug("Create alluxio file {} with checkpoint location {}", tfsPath, ufsPath);
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
            if (checkPath.startsWith(AlluxioURI.SEPARATOR)) {
              checkPath = checkPath.substring(AlluxioURI.SEPARATOR.length());
            }
            if (excludePathPrefix.inList(checkPath)) {
              LOG.debug("excluded: {}", checkPath);
            } else {
              ufsPathQueue.add(new AlluxioURI(aPath));
            }
          }
        }
        // ufsPath is a directory, so only concat the tfsRoot with the relative path
        AlluxioURI tfsPath = new AlluxioURI(PathUtils.concatPath(
            tachyonPath, ufsPath.getPath().substring(ufsAddrRootPath.getPath().length())));
        LOG.debug("Loading ufs. ufs path is a directory. tfsPath = {}.", tfsPath);
        if (!fs.exists(tfsPath)) {
          LOG.debug("Loading ufs. ufs path is a directory. make dir = {}.", tfsPath);
          fs.loadMetadata(tfsPath);
          // TODO(hy): Add the following.
          // if (fs.mkdir(tfsPath)) {
          // LOG.info("Created mTachyonFS folder {} with checkpoint location {}", tfsPath, ufsPath);
          // } else {
          // LOG.info("Failed to create alluxio folder: {}", tfsPath);
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
      loadUfs(new AlluxioURI(args[0]), new AlluxioURI(args[1]), exList, new Configuration());
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
    String cmd = "java -cp " + Version.TACHYON_JAR + " alluxio.client.UfsUtils ";

    System.out.println("Usage: " + cmd + "<TachyonPath> <UfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a /b c");
    System.out.print("In the TFS, all files under local FS /b will be registered under /a, ");
    System.out.println("except for those with prefix c");
  }

  private UfsUtils() {} // prevent instantiation
}
