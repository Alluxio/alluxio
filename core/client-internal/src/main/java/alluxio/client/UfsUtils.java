/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.Version;
import alluxio.client.file.FileSystem;
import alluxio.collections.PrefixList;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.underfs.UnderFileSystem;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utilities related to under file system.
 */
@ThreadSafe
public final class UfsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Loads files under path "ufsAddrRootPath" (excluding excludePathPrefix relative to the path) to
   * the given tfs under a given destination path.
   *
   * @param tfsAddrRootPath the Alluxio file system address and path to load the src files, like
   *        "alluxio://host:port/dest".
   * @param ufsAddrRootPath the address and root path of the under file system, like
   *        "hdfs://host:port/src"
   * @param excludePaths paths to exclude from ufsRootPath, which will not be loaded in Alluxio file
   *        system
   * @param configuration the instance of {@link Configuration} to be used
   * @throws IOException when an event that prevents the operation from completing is encountered
   * @throws AlluxioException if an unexpected Alluxio error occurs
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
   * @param mountPointUri the destination point in Alluxio file system to load the under file system
   *        path onto
   * @param mountDirectoryUri the address and root path of the under file system, like
   *        "hdfs://host:port/dir"
   * @param exclusions paths to exclude from ufsRootPath, which will not be registered in
   *        Alluxio file system
   * @param conf instance of {@link Configuration}
   * @throws IOException when an event that prevents the operation from completing is encountered
   * @throws AlluxioException if an unexpected alluxio error occurs
   */
  public static void loadUfs(FileSystem fs, AlluxioURI mountPointUri, AlluxioURI mountDirectoryUri,
      PrefixList exclusions, Configuration conf) throws AlluxioException, IOException {
    LOG.info("Mounting: {} to Alluxio directory: {}, excluding: {}", mountDirectoryUri,
        mountPointUri, exclusions);

    UnderFileSystem ufs = UnderFileSystem.get(mountDirectoryUri.toString(), conf);
    // The mount directory must exist in the UFS
    String mountDirectoryPath = mountDirectoryUri.getPath();
    if (!ufs.exists(mountDirectoryPath)) {
      throw new FileDoesNotExistException(
          ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(mountDirectoryUri));
    }
    // Mount the ufs directory to the Alluxio mount point
    try {
      fs.mount(mountPointUri, mountDirectoryUri);
    } catch (InvalidPathException e) {
      // If the mount point already exists, ignore it and assume the mount is consistent
      if (!ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(mountPointUri)
          .equals(e.getMessage())) {
        throw e;
      }
      LOG.warn("Mount point: " + mountPointUri + " has already been mounted! Assuming the mount "
          + "is consistent and proceeding to load metadata.");
    }
    // Get the list of files to load
    List<String> files = Arrays.asList(ufs.listRecursive(mountDirectoryPath));
    for (String file : files) {
      LOG.info("Considering file: " + file);
      // Not excluded
      if (exclusions.outList(file)) {
        AlluxioURI alluxioUri = mountPointUri.join(file);
        LOG.info("Loading metadata for file: " + alluxioUri);
        // TODO(calvin): Remove the need for this hack
        AlluxioURI alluxioPath = new AlluxioURI(alluxioUri.getPath());
        if (!fs.exists(alluxioPath)) {
          fs.loadMetadata(alluxioPath);
        }
      }
    }
  }

  /**
   * Starts the command line utility to load files under path "ufsAddress/ufsRootPath"
   * (excluding excludePathPrefix) to the given tfs under the given tfsRootPath directory.
   *
   * @param args the parameters as <AlluxioPath> <UfsPath> [<Optional ExcludePathPrefix, seperated
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
    String cmd = "java -cp " + Version.ALLUXIO_JAR + " alluxio.client.UfsUtils ";

    System.out.println("Usage: " + cmd + "<AlluxioPath> <UfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a /b c");
    System.out.print("In the TFS, all files under local FS /b will be registered under /a, ");
    System.out.println("except for those with prefix c");
  }

  private UfsUtils() {} // prevent instantiation
}
