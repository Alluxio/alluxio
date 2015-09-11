/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.client.file;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.ClientOptions;
import tachyon.client.FileSystemMasterClient;
import tachyon.thrift.FileInfo;

/**
 * Tachyon File System client. This class is the entry point for all file level operations on
 * Tachyon files. An instance of this class can be obtained via {@link TachyonFileSystem#get}. This
 * class is thread safe. The read/write interface provided by this client is similar to Java's
 * input/output streams.
 */
public final class TachyonFileSystem implements Closeable, TachyonFSCore {
  /** The single instance of the TachyonFileSystem */
  private static TachyonFileSystem sClient;

  /**
   * @return a TachyonFileSystem instance, there is only one instance available at any time
   */
  public static synchronized TachyonFileSystem get() {
    if (sClient == null) {
      sClient = new TachyonFileSystem();
    }
    return sClient;
  }

  /** The file system context which contains shared resources, such as the fs master client */
  private FileSystemContext mContext;

  /**
   * Constructor, currently TachyonFileSystem does not retain any state
   */
  private TachyonFileSystem() {
    mContext = FileSystemContext.INSTANCE;
  }

  /**
   * Closes this TachyonFS instance. The next call to get will create a new TachyonFS instance.
   * Other references to the old client may still be used.
   */
  // TODO(calvin): Evaluate the necessity of this method.
  @Override
  public synchronized void close() {
    sClient = null;
  }

  /**
   * Deletes a file. If the file is a folder, its contents will be deleted recursively. The delete
   * will abort on a failure, but previous deletes that occurred will still be effective. The delete
   * will only synchronously be propagated to the master. The file metadata will not be available
   * after this call, but the data in Tachyon or under storage space may still reside until the
   * delete is propagated.
   *
   * @param file the handler of the file to delete.
   * @throws IOException if the master is unable to delete the file
   */
  @Override
  public void delete(TachyonFile file) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.deleteFile(file.getFileId(), true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Removes the file from Tachyon Storage. The underlying under storage system file will not be
   * removed. If the file is a folder, its contents will be freed recursively. This method is
   * asynchronous and will be propagated to the workers through their heartbeats.
   *
   * @param file the handler for the file
   * @throws IOException if the master is unable to free the file
   */
  @Override
  public void free(TachyonFile file) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.free(file.getFileId(), true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets the FileInfo object that represents the Tachyon file. The file info is a snapshot of the
   * file metadata, and the locations, last modified time, and path are possibly inconsistent.
   *
   * @param file the handler for the file.
   * @return the FileInfo of the file, null if the file does not exist.
   * @throws IOException if the master is unable to obtain the file's metadata
   */
  // TODO(calvin): Consider FileInfo caching.
  @Override
  public FileInfo getInfo(TachyonFile file) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfo(file.getFileId());
    } catch (IOException e) {
      return null;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets a {@link FileInStream} for the specified file. The stream's settings can be customized by
   * setting the options parameter. The user should close the stream after finishing the operations
   * on it.
   *
   * @param file the handler for the file.
   * @param options the set of options specific to this operation.
   * @return an input stream to read the file
   * @throws IOException if the file does not exist or the stream cannot be opened
   */
  public FileInStream getInStream(TachyonFile file, ClientOptions options) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      // TODO(calvin): Make sure the file is not a folder.
      FileInfo info = masterClient.getFileInfo(file.getFileId());
      if (info.isFolder) {
        throw new IOException("Cannot get an instream to a folder.");
      }
      return new FileInStream(info, options);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Creates a file and gets the {@link FileOutStream} for the specified file. This should only be
   * called to write a file that does not exist. Once close is called on the output stream, the file
   * will be completed. Append or update of a completed file is currently not supported.
   *
   * @param path the Tachyon path of the file
   * @param options the set of options specific to this operation
   * @return an output stream to write the file
   * @throws IOException if the file already exists or if the stream cannot be opened
   */
  public FileOutStream getOutStream(TachyonURI path, ClientOptions options) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long fileId = masterClient.createFile(path.getPath(), options.getBlockSize(), true);
      return new FileOutStream(fileId, options);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  // TODO(calvin): We should remove this when the TachyonFS code is fully deprecated.
  @Deprecated
  public FileOutStream getOutStream(long fileId, ClientOptions options) throws IOException {
    return new FileOutStream(fileId, options);
  }

  /**
   * If the file is a folder, return the {@link FileInfo} of all the direct entries in it. Otherwise
   * return the FileInfo for the file. The file infos are snapshots of the file metadata, and the
   * locations, last modified time, and path are possibly inconsistent.
   *
   * @param file the handler for the file
   * @return a list of FileInfos representing the files which are children of the given file
   * @throws IOException if the file does not exist
   */
  @Override
  public List<FileInfo> listStatus(TachyonFile file) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfoList(file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Adds metadata about a file in the under storage system to Tachyon. Only metadata will be
   * updated and no data will be transferred. The data can be added to Tachyon space by doing an
   * operation with the cache option specified, for example reading.
   *
   * @param path the path to create the file in Tachyon
   * @param ufsPath the under storage system path of the file that will back the Tachyon file
   * @param recursive if true, the parent directories to the file in Tachyon will be created
   * @return the file id of the resulting file in Tachyon
   * @throws IOException if the Tachyon path is invalid or the ufsPath does not exist
   */
  public long loadFileInfoFromUfs(TachyonURI path, TachyonURI ufsPath, boolean recursive)
      throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.loadFileInfoFromUfs(path.getPath(), ufsPath.toString(), -1L, recursive);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Creates a folder. If the parent folders do not exist, they will be created automatically.
   *
   * @param path the handler for the file
   * @return true if the folder is created successfully or already existing, false otherwise.
   * @throws IOException if the master cannot create the folder under the specified path
   */
  @Override
  public boolean mkdirs(TachyonURI path) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      // TODO: Change this RPC's arguments
      return masterClient.createDirectory(path.getPath(), true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Resolves a {@link TachyonURI} to a {@link TachyonFile} which is used as the file handler for
   * non-create operations.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @return a TachyonFile which acts as a file handler for the path
   * @throws IOException if the path does not exist in Tachyon space
   */
  @Override
  public TachyonFile open(TachyonURI path) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return new TachyonFile(masterClient.getFileId(path.getPath()));
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Sets the pin status of a file. A pinned file will never be evicted for any reason. The pin
   * status is propagated asynchronously from this method call on the worker heartbeats.
   *
   * @param file the file handler for the file to pin
   * @param pinned true to pin the file, false to unpin it
   * @throws IOException if an error occurs during the pin operation
   */
  @Override
  public void setPin(TachyonFile file, boolean pinned) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.setPinned(file.getFileId(), pinned);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Renames an existing file in Tachyon space to another path in Tachyon space.
   *
   * @param src The file handler for the source file
   * @param dst The path of the destination file, this path should not exist
   * @return true if successful, false otherwise
   * @throws IOException if the destination already exists or is invalid
   */
  @Override
  public boolean rename(TachyonFile src, TachyonURI dst) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.renameFile(src.getFileId(), dst.getPath());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  public void reportLostFile(TachyonFile file) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.reportLostFile(file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  public void requestFilesInDependency(int depId) throws IOException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.requestFilesInDependency(depId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
