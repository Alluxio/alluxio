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

package alluxio.service.file;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.service.file.options.GetStatusOptions;
import alluxio.wire.FileInfo;

import java.io.IOException;

/**
 * The interface of file system master service.
 */
public interface FileSystemMasterService {
    /**
   * Returns the {@link FileInfo} for a given path.
   * <p>
   * This operation requires users to have READ permission on the path.
   *
   * @param path the path to get the {@link FileInfo} for
   * @param options the {@link GetStatusOptions}
   * @return the {@link FileInfo} for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if the file path is not valid
   * @throws AccessControlException if permission checking fails
   */
  FileInfo getFileInfo(AlluxioURI path, GetStatusOptions options)
      throws FileDoesNotExistException, InvalidPathException, AccessControlException,
      UnavailableException, IOException;

}
