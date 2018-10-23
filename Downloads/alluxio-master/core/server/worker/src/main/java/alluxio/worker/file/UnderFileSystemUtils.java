/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.worker.file;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.security.authorization.Mode;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.options.MkdirsOptions;

import java.io.IOException;
import java.util.Stack;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility functions for working with {@link alluxio.underfs.UnderFileSystem}.
 */
@ThreadSafe
public final class UnderFileSystemUtils {

  /**
   * Creates parent directories for path with correct permissions if required.
   *
   * @param alluxioPath Alluxio path
   * @param ufsPath path in the under file system
   * @param fs file system master client
   * @param ufs the under file system
   */
  public static void prepareFilePath(AlluxioURI alluxioPath, String ufsPath, FileSystem fs,
      UnderFileSystem ufs) throws AlluxioException, IOException {
    AlluxioURI dstPath = new AlluxioURI(ufsPath);
    String parentPath = dstPath.getParent().getPath();
    // creates the parent folder if it does not exist
    if (!ufs.isDirectory(parentPath)) {
      // Create ancestor directories from top to the bottom. We cannot use recursive create parents
      // here because the permission for the ancestors can be different.
      Stack<Pair<String, MkdirsOptions>> ufsDirsToMakeWithOptions = new Stack<>();
      AlluxioURI curAlluxioPath = alluxioPath.getParent();
      AlluxioURI curUfsPath = dstPath.getParent();
      // Stop at Alluxio mount point because the mapped directory in UFS may not exist.
      while (curUfsPath != null && !ufs.isDirectory(curUfsPath.toString())
          && curAlluxioPath != null) {
        URIStatus curDirStatus = fs.getStatus(curAlluxioPath);
        if (curDirStatus.isMountPoint()) {
          throw new IOException(ExceptionMessage.UFS_PATH_DOES_NOT_EXIST.getMessage(curUfsPath));
        }
        ufsDirsToMakeWithOptions.push(new Pair<>(curUfsPath.toString(),
            MkdirsOptions.defaults().setCreateParent(false).setOwner(curDirStatus.getOwner())
                .setGroup(curDirStatus.getGroup())
                .setMode(new Mode((short) curDirStatus.getMode()))));
        curAlluxioPath = curAlluxioPath.getParent();
        curUfsPath = curUfsPath.getParent();
      }
      while (!ufsDirsToMakeWithOptions.empty()) {
        Pair<String, MkdirsOptions> ufsDirAndPerm = ufsDirsToMakeWithOptions.pop();
        // UFS mkdirs might fail if the directory is already created. If so, skip the mkdirs
        // and assume the directory is already prepared, regardless of permission matching.
        if (!ufs.mkdirs(ufsDirAndPerm.getFirst(), ufsDirAndPerm.getSecond())
            && !ufs.isDirectory(ufsDirAndPerm.getFirst())) {
          throw new IOException("Failed to create dir: " + ufsDirAndPerm.getFirst());
        }
      }
    }
  }

  private UnderFileSystemUtils() {} // prevent instantiation
}
