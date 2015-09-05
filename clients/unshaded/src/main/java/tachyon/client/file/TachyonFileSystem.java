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
import tachyon.annotation.PublicApi;
import tachyon.client.ClientOptions;
import tachyon.client.FileSystemMasterClient;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;

/**
 * Tachyon File System client. This class is the entry point for all file level operations on
 * Tachyon files. An instance of this class can be obtained via {@link TachyonFileSystem#get}. This
 * class is thread safe. The read/write interface provided by this client is similar to Java's
 * input/output streams.
 */
@PublicApi
public class TachyonFileSystem implements Closeable, TachyonFSCore {
  /** A cached instance of the TachyonFileSystem */
  private static TachyonFileSystem sClient;

  /**
   * @return a TachyonFileSystem instance, there is only one instance available at any time
   */
  public static synchronized TachyonFileSystem get() {
    if (null == sClient) {
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
   * {@inheritDoc} The delete will abort on a failure, but previous deletes that occurred will still
   * be effective. The delete will only synchronously be propagated to the master. The file metadata
   * will not be available after this call, but the data in Tachyon or under storage space may still
   * reside until the delete is propagated.
   */
  @Override
  public void delete(TachyonFile file) throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.deleteFile(file.getFileId(), true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc} This method is asynchronous and will be propagated to the workers through their
   * heartbeats.
   */
  @Override
  public void free(TachyonFile file) throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.free(file.getFileId(), true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc} The file info is a snapshot of the file metadata, and the locations, last
   * modified time, and path are possibly inconsistent.
   */
  // TODO(calvin): Consider FileInfo caching.
  @Override
  public FileInfo getInfo(TachyonFile file) throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.getFileInfo(file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets a {@link FileInStream} for the specified file. The stream's settings can be customized by
   * setting the options parameter. The caller should close the stream after finishing the
   * operations on it.
   *
   * @param file the handler for the file.
   * @param options the set of options specific to this operation.
   * @return an input stream to read the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if the stream cannot be opened for some other reason
   */
  public FileInStream getInStream(TachyonFile file, ClientOptions options) throws IOException,
      FileDoesNotExistException {
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
   * @throws InvalidPathException if the provided path is invalid
   * @throws FileAlreadyExistException if the file being written to already exists
   * @throws BlockInfoException if the provided block size is invalid
   * @throws IOException if the file already exists or if the stream cannot be opened
   */
  public FileOutStream getOutStream(TachyonURI path, ClientOptions options) throws IOException,
      InvalidPathException, FileAlreadyExistException, BlockInfoException {
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
   * {@inheritDoc} The file infos are snapshots of the file metadata, and the locations, last
   * modified time, and path are possibly inconsistent.
   */
  @Override
  public List<FileInfo> listStatus(TachyonFile file) throws IOException, FileDoesNotExistException {
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
   * @throws FileDoesNotExistException if there is no file at the given path
   * @throws IOException if the Tachyon path is invalid or the ufsPath does not exist
   */
  public long loadFileInfoFromUfs(TachyonURI path, TachyonURI ufsPath, boolean recursive)
      throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.loadFileInfoFromUfs(path.getPath(), ufsPath.toString(), -1L, recursive);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean mkdirs(TachyonURI path) throws IOException, InvalidPathException,
      FileAlreadyExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      // TODO: Change this RPC's arguments
      return masterClient.createDirectory(path.getPath(), true);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TachyonFile open(TachyonURI path) throws IOException, InvalidPathException {
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
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an error occurs during the pin operation
   */
  public void setPin(TachyonFile file, boolean pinned) throws IOException,
      FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.setPinned(file.getFileId(), pinned);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean rename(TachyonFile src, TachyonURI dst) throws IOException,
      FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      return masterClient.renameFile(src.getFileId(), dst.getPath());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  public void reportLostFile(TachyonFile file) throws IOException, FileDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.reportLostFile(file.getFileId());
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  public void requestFilesInDependency(int depId) throws IOException,
      DependencyDoesNotExistException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.requestFilesInDependency(depId);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
