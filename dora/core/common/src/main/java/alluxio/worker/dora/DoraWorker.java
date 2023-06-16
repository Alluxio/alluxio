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

package alluxio.worker.dora;

import alluxio.exception.AccessControlException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.DeletePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.LoadFileFailure;
import alluxio.grpc.RenamePOptions;
import alluxio.grpc.Route;
import alluxio.grpc.RouteFailure;
import alluxio.grpc.UfsReadOptions;
import alluxio.grpc.WriteOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.underfs.UfsStatus;
import alluxio.wire.FileInfo;
import alluxio.worker.DataWorker;
import alluxio.worker.SessionCleanable;
import alluxio.worker.block.io.BlockReader;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A block worker in the Alluxio system.
 */
public interface DoraWorker extends DataWorker, SessionCleanable {
  /**
   * Gets the file information.
   *
   * @param fileId the file id
   * @param options the options for the GetStatusPRequest
   * @return the file info
   */
  FileInfo getFileInfo(String fileId, GetStatusPOptions options)
      throws IOException, AccessControlException;

  /**
   * List status from Under File System.
   *
   * Please refer to UnderFileSystem.listStatus().
   *
   * @param path the path of a dir or file
   * @param options the option for listStatus()
   * @return An array with the statuses of the files and directories in the directory denoted by
   *         this abstract pathname. The array will be empty if the directory is empty. Returns
   *         {@code null} if this abstract pathname does not denote a directory.
   * @throws IOException
   */
  @Nullable
  UfsStatus[] listStatus(String path, ListStatusPOptions options)
      throws IOException, AccessControlException;

  /**
   * Creates the file reader to read from Alluxio dora.
   * Owner of this block reader must close it or lock will leak.
   *
   * @param fileId the ID of the UFS file
   * @param offset the offset within the block
   * @param positionShort whether the operation is using positioned read to a small buffer size
   * @param options the options
   * @return a block reader to read data from
   * @throws IOException if it fails to get block reader
   */
  BlockReader createFileReader(String fileId, long offset,
      boolean positionShort, Protocol.OpenUfsBlockOptions options)
      throws IOException, AccessControlException;

  /**
   * Creates the file writer to write to Alluxio dora.
   * Owner of this block writer must close it or lock will leak.
   *
   * @param fileId  the ID of the UFS file
   * @param ufsPath the path of UFS file
   * @return the block writer for the local file
   */
  BlockWriter createFileWriter(String fileId, String ufsPath)
      throws AccessControlException, IOException;

  /**
   * Loads the metadata and data of files from UFS to Alluxio.
   *
   * @param loadData true if data should also be loaded, otherwise metadata only
   * @param ufsStatuses the files to load
   * @param options
   * @return a list of failed files
   */
  ListenableFuture<List<LoadFileFailure>> load(
      boolean loadData, List<UfsStatus> ufsStatuses, UfsReadOptions options)
      throws AccessControlException, IOException;

  /**
   * Copies files from src to dst.
   *
   * @param routes   the files to copy
   * @param readOptions the options for reading
   * @param writeOptions the options for writing
   * @return a list of failed files
   */
  ListenableFuture<List<RouteFailure>> copy(List<Route> routes, UfsReadOptions readOptions,
      WriteOptions writeOptions);

  /**
   * Moves files from src to dst.
   *
   * @param routes the files to move
   * @param readOptions the options for reading
   * @param writeOptions the options for writing
   * @return a list of failed files
   */
  ListenableFuture<List<RouteFailure>> move(List<Route> routes, UfsReadOptions readOptions,
                                            WriteOptions writeOptions);

  /**
   * Create File.
   * @param path the path of this file
   * @param options the options for this operation
   * @return a open file handle including a unique uuid
   */
  OpenFileHandle createFile(String path, CreateFilePOptions options)
      throws AccessControlException, IOException;

  /**
   * Complete writing a file. This operation will verify the supplied uuid matching its open handle.
   * @param path the path of this file
   * @param options the options for this operation
   * @param uuid the uuid string of its file open handle
   */
  void completeFile(String path, CompleteFilePOptions options, String uuid)
      throws IOException, AccessControlException;

  /**
   * Delete a file.
   * @param path the path of this file
   * @param options the options for this operation
   */
  void delete(String path, DeletePOptions options) throws IOException, AccessControlException;

  /**
   * Rename src to dst.
   * @param src the source file/dir
   * @param dst the destination file/dir
   * @param options the options for this operations
   */
  void rename(String src, String dst, RenamePOptions options)
      throws IOException, AccessControlException;

  /**
   * Create a directory.
   * @param path the directory name
   * @param options the options for this operations
   */
  void createDirectory(String path, CreateDirectoryPOptions options)
      throws IOException, AccessControlException;
}
