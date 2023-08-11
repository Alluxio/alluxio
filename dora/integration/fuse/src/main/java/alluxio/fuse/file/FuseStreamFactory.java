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

package alluxio.fuse.file;

import alluxio.AlluxioURI;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for {@link FuseFileInStream}.
 */
@ThreadSafe
public interface FuseStreamFactory {

  /**
   * Factory method for creating/opening a file
   * and creating an implementation of {@link FuseFileStream}.
   *
   * @param uri the Alluxio URI
   * @param flags the create/open flags
   * @param mode the create file mode, -1 if not set
   * @return the created fuse file stream
   */
  FuseFileStream create(AlluxioURI uri, int flags, long mode);
}
