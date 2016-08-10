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
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.security.authorization.Permission;
import alluxio.worker.Worker;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

/**
 * Interface representing a file system worker.
 */
public interface FileSystemWorker extends Worker {

  /**
   * Cancels a file currently being written to the under file system. The open stream will be
   * closed and the partial file will be cleaned up.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the id of the file to cancel, only understood by the worker that created
   *                      the file
   * @throws FileDoesNotExistException if this worker is not writing the specified file
   * @throws IOException if an error occurs interacting with the under file system
   */
  void cancelUfsFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException;

  /**
   * Closes a file currently being read from the under file system. The open stream will be
   * closed and the file id will no longer be valid.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the id of the file to close, only understood by the worker that opened
   *                      the file
   * @throws FileDoesNotExistException if the worker is not reading the specified file
   * @throws IOException if an error occurs interacting with the under file system
   */
  void closeUfsFile(long sessionId, long tempUfsFileId)
      throws FileDoesNotExistException, IOException;

  /**
   * Completes a file currently being written to the under file system. The open stream will be
   * closed and the partial file will be promoted to the completed file in the under file system.
   *
   * @param sessionId the session id of the request
   * @param tempUfsFileId the id of the file to complete, only understood by the worker that created
   *                      the file
   * @param perm the permission of the file
   * @return the length of the completed file
   * @throws FileDoesNotExistException if the worker is not writing the specified file
   * @throws IOException if an error occurs interacting with the under file system
   */
  long completeUfsFile(long sessionId, long tempUfsFileId, Permission perm)
      throws FileDoesNotExistException, IOException;

  /**
   * Creates a new file in the under file system. This will register a new stream in the under
   * file system manager. The stream can only be accessed with the returned id afterward.
   *
   * @param sessionId the session id of the request
   * @param ufsUri the under file system uri to create a file for
   * @param perm the permission of the file
   * @throws FileAlreadyExistsException if a file already exists in the under file system with
   *                                    the same path
   * @throws IOException if an error occurs interacting with the under file system
   * @return the temporary worker specific file id which references the in-progress ufs file
   */
  long createUfsFile(long sessionId, AlluxioURI ufsUri, Permission perm)
      throws FileAlreadyExistsException, IOException;

  /**
   * Opens a stream to the under file system file denoted by the temporary file id. This call
   * will skip to the position specified in the file before returning the stream. The caller of
   * this method should not close this stream. Resources will automatically be cleaned up when
   * {@link #closeUfsFile(long, long)} is called.
   *
   * @param tempUfsFileId the worker specific temporary file id for the file in the under storage
   * @param position the absolute position in the file to position the stream at before returning
   * @return an input stream to the ufs file positioned at the given position, null if the
   *         position is past the end of the file.
   * @throws FileDoesNotExistException if the worker file id is invalid
   * @throws IOException if an error occurs interacting with the under file system
   */
  InputStream getUfsInputStream(long tempUfsFileId, long position)
      throws FileDoesNotExistException, IOException;

  /**
   * Returns the output stream to the under file system file denoted by the temporary file id.
   * The stream should not be closed by the caller but through the {@link #cancelUfsFile(long,long)}
   * or the {@link #completeUfsFile(long, long, String, String)} methods.
   *
   * @param tempUfsFileId the worker specific temporary file id for the file in the under storage
   * @return the output stream writing the contents of the file
   * @throws FileDoesNotExistException if the temporary file id is invalid
   */
  OutputStream getUfsOutputStream(long tempUfsFileId) throws FileDoesNotExistException;

  /**
   * @return the worker service handler
   */
  FileSystemWorkerClientServiceHandler getWorkerServiceHandler();

  /**
   * Opens a file in the under file system and registers a temporary file id with the file. This
   * id is valid until the file is closed.
   *
   * @param sessionId the session id of the request
   * @param ufsUri the under file system path of the file to open
   * @return the temporary file id which references the file
   * @throws FileDoesNotExistException if the file does not exist in the under file system
   * @throws IOException if an error occurs interacting with the under file system
   */
  long openUfsFile(long sessionId, AlluxioURI ufsUri) throws FileDoesNotExistException, IOException;

  /**
   * Registers a client's heartbeat to keep its session alive. The client can also
   * piggyback metrics on this call. Currently there are no metrics collected from this call.
   *
   * @param sessionId the session id to renew
   * @param metrics a list of metrics to update from the client
   */
  void sessionHeartbeat(long sessionId, List<Long> metrics);
}
