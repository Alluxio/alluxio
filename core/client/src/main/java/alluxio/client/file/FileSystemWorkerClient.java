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

package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.client.file.options.CancelUfsFileOptions;
import alluxio.client.file.options.CloseUfsFileOptions;
import alluxio.client.file.options.CompleteUfsFileOptions;
import alluxio.client.file.options.CreateUfsFileOptions;
import alluxio.client.file.options.OpenUfsFileOptions;
import alluxio.exception.AlluxioException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Interface for an Alluxio block worker client.
 */
public interface FileSystemWorkerClient extends Closeable {

  /**
   * Cancels the file currently being written with the specified id. This file must have also
   * been created through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to cancel
   * @param options method options
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  void cancelUfsFile(final long tempUfsFileId, final CancelUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * Closes the file currently being written with the specified id. This file must have also
   * been opened through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to close
   * @param options method options
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  void closeUfsFile(final long tempUfsFileId, final CloseUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * Completes the file currently being written with the specified id. This file must have also
   * been created through this client. The file id will be invalid after this call.
   *
   * @param tempUfsFileId the worker specific id of the file to complete
   * @param options method options
   * @return the file size of the completed file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  long completeUfsFile(final long tempUfsFileId, final CompleteUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * Creates a new file in the UFS with the given path.
   *
   * @param path the path in the UFS to create, must not already exist
   * @param options method options
   * @return the worker specific file id to reference the created file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  long createUfsFile(final AlluxioURI path, final CreateUfsFileOptions options)
      throws AlluxioException, IOException;

  /**
   * @return the data server address of the worker this client is connected to
   */
  InetSocketAddress getWorkerDataServerAddress();

  /**
   * Opens an existing file in the UFS with the given path.
   *
   * @param path the path in the UFS to open, must exist
   * @param options method options
   * @return the worker specific file id to reference the opened file
   * @throws AlluxioException if an error occurs in the internals of the Alluxio worker
   * @throws IOException if an error occurs interacting with the UFS
   */
  long openUfsFile(final AlluxioURI path, final OpenUfsFileOptions options)
      throws AlluxioException, IOException;
  /**
   * Sends a session heartbeat to the worker. This renews the client's lease on resources such as
   * temporary files.
   *
   * @throws IOException if an I/O error occurs
   * @throws InterruptedException if the heartbeat is interrupted
   */
  void sessionHeartbeat() throws IOException, InterruptedException;
}
