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
   * Mounts the given folder in the under storage system to an Alluxio path which does not exist.
   * Then loads metadata for each file in the under file system unless excluded by the exclusions
   * list. If the folder has already been mounted, this method assumes the mount is correct and
   * will continue to load metadata for each file. This method can be called repeatedly to sync
   * the latest metadata from the under file system.
   *
   * @param mountPointUri the destination point in Alluxio file system to load the under file system
   *        path onto, this must not already exist
   * @param mountDirectoryUri the directory in the under file system to mount
   * @param exclusions prefixes to exclude which will not be registered in the Alluxio file system
   * @param conf instance of {@link Configuration}
   * @throws IOException if an error occurs when operating on the under file system
   * @throws AlluxioException if an unexpected Alluxio error occurs
   */
  public static void loadUfs(AlluxioURI mountPointUri, AlluxioURI mountDirectoryUri,
      PrefixList exclusions, Configuration conf) throws AlluxioException, IOException {
    FileSystem fs = FileSystem.Factory.get();
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
   * Starts the command line utility to mount the UfsPath to the AlluxioPath. In addition to the
   * regular mount operation, the metadata for all files will be loaded, unless they match the
   * exclusions prefix list. This utility can be called again on an already mounted directory to
   * sync with the under file system.
   *
   * NOTE: The user must guarantee additional calls to the utility do not change the Alluxio/Ufs
   * Path pairing.
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

    System.out.println("Usage: " + cmd + "<AlluxioPath> <UfsPath> "
        + "[<Optional ExcludePathPrefix, separated by ;>]");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a hdfs://localhost:9000/b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a file:///b c");
    System.out.println("Example: " + cmd + "alluxio://127.0.0.1:19998/a /b c");
    System.out.println("In the Alluxio file system, /b will be mounted to /a, and the metadata");
    System.out.println("for all files under /b will be loaded except for those with prefix c");
  }

  private UfsUtils() {} // prevent instantiation
}
