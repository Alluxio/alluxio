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

package alluxio.underfs;

import java.util.Optional;

/**
 * Interface for returning the content hash. Instances of {@link java.io.OutputStream} returned by
 * {@link UnderFileSystem#create} may implement this interface if the UFS returns the hash of the
 * content written when the stream is closed. The content hash will then be used as part of
 * the metadata fingerprint when the file is completed on the Alluxio master.
 */
public interface UnderFileSystemOutputStream {
  /**
   * @return the content hash of the file written to the UFS if available
   * after the stream has been closed
   */
  Optional<String> getContentHash();
}
