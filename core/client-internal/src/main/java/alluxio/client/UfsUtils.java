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
   * Mounts the given folder in the under storage system to an Alluxio path which must not already
   * exist. Then loads metadata for each file in the under file system unless excluding any file
   * in the under file system matching a prefix in the exclusions list. If the folder has already
   * been mounted, this method assumes the mount is correct and will continue to load metadata
   * for each file. This method can be called repeatedly to load new files written directly to
   * the under file system. File deletions will not be reflected, ie. deleting a file, foo, in
   * the under file system and then running loadUfs will not delete the file foo in Alluxio.
   *
   * @param alluxioUri the destination point in Alluxio file system to load the under file system
   *        path onto, this must not already exist
   * @param ufsUri the directory in the under file system to mount
   * @param exclusions prefixes to exclude which will not be registered in the Alluxio file
   *                   system, these should be relative to the under file system path and not the
   *                   Alluxio path
   * @param conf instance of {@link Configuration}
   * @throws IOException if an error occurs when operating on the under file system
   * @throws AlluxioException if an unexpected Alluxio error occurs
   * @deprecated This utility will be replaced with the mount and recursive load metadata
   *             operations.
   */
  @Deprecated
  public static void loadUfs(AlluxioURI alluxioUri, AlluxioURI ufsUri,
      PrefixList exclusions, Configuration conf) throws AlluxioException, IOException {
    FileSystem fs = FileSystem.Factory.get();
    LOG.info("Mounting: {} to Alluxio directory: {}, excluding: {}", ufsUri,
        alluxioUri, exclusions);

    UnderFileSystem ufs = UnderFileSystem.get(ufsUri.toString(), conf);
    // The mount directory must exist in the UFS
    String mountDirectoryPath = ufsUri.getPath();
    if (!ufs.exists(mountDirectoryPath)) {
      throw new FileDoesNotExistException(
          ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(ufsUri));
    }
    // Mount the ufs directory to the Alluxio mount point
    try {
      fs.mount(alluxioUri, ufsUri);
    } catch (InvalidPathException e) {
      // If the mount point already exists, ignore it and assume the mount is consistent
      if (!ExceptionMessage.MOUNT_POINT_ALREADY_EXISTS.getMessage(alluxioUri)
          .equals(e.getMessage())) {
        throw e;
      }
      LOG.warn("Mount point: " + alluxioUri + " has already been mounted! Assuming the mount "
          + "is consistent and proceeding to load metadata.");
    }
    // Get the list of files to load
    List<String> files = Arrays.asList(ufs.listRecursive(mountDirectoryPath));
    for (String file : files) {
      LOG.debug("Considering ufs path: " + file);
      // Not excluded
      if (exclusions.outList(file)) {
        AlluxioURI alluxioUriToLoad = alluxioUri.join(file);
        LOG.debug("Loading metadata for Alluxio uri: " + alluxioUriToLoad);
        // TODO(calvin): Remove the need for this hack
        AlluxioURI alluxioPath = new AlluxioURI(alluxioUriToLoad.getPath());
        if (!fs.exists(alluxioPath)) {
          fs.loadMetadata(alluxioPath);
        }
      }
    }
  }

  /**
   * Starts the command line utility to mount the UfsPath to the AlluxioPath. In addition to the
   * regular mount operation, the metadata for all files will be loaded, unless the ufs path matches
   * an exclusion in the exclusions prefix list. This utility can be called repeatedly to load new
   * files written directly to the under file system. File deletions will not be reflected, ie.
   * deleting a file, foo, in the under file system and then running loadUfs will not delete the
   * file foo in Alluxio.
   *
   * NOTE: The user must guarantee additional calls to the utility do not change the Alluxio/Ufs
   * Path pairing.
   *
   * @param args the parameters as <AlluxioUri> <UfsUri> [<Optional ExcludePathPrefix, seperated
   *             by ;>]
   */
  public static void main(String[] args) {
    if (!(args.length == 2 || args.length == 3)) {
      printUsage();
      System.exit(-1);
    }

    String exList = (args.length == 3) ? args[2] : "";

    try {
      loadUfs(new AlluxioURI(args[0]), new AlluxioURI(args[1]), new PrefixList(exList, ";"),
          new Configuration());
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

    System.out.println("Usage: " + cmd + "<AlluxioUri> <UfsUri> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a /b c");
    System.out.println("In the Alluxio file system, /b will be mounted to /a, and the metadata");
    System.out.println("for all files under /b will be loaded except for those with prefix c");
  }

  private UfsUtils() {} // prevent instantiation
}
