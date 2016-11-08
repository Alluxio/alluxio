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

import alluxio.underfs.options.CreateOptions;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link UnderFileSystem} which want to use a {@link NonAtomicFileOutputStream}.
 */
public interface NonAtomicUnderFileSystem {
  /**
   * Create an output stream to a temporary path.
   *
   * @param path temporary path
   * @param options the options for create
   * @return a non atomic output stream
   * @throws IOException when create fails
   */
  OutputStream createInternal(String path, CreateOptions options) throws IOException;

  /**
   * Delete temporary path.
   *
   * @param path temporary path
   * @param recursive is false
   * @return true if delete was successful
   * @throws IOException if a non Alluxio error occurs
   */
  boolean delete(String path, boolean recursive) throws IOException;

  /**
   * Rename temporary path to final destination.
   *
   * @param src temporary path
   * @param dst final path
   * @return true if rename was successful
   * @throws IOException if a non Alluxio error occurs
   */
  boolean rename(String src, String dst) throws IOException;

  /**
   *
   * @param path final path
   * @param owner owner
   * @param group group
   * @throws IOException if a non Alluxio error occurs
   */
  void setOwner(String path, String owner, String group) throws IOException;
}

