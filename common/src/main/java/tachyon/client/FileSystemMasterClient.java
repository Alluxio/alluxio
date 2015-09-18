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

package tachyon.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.MasterClientBase;
import tachyon.conf.TachyonConf;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.DependencyInfo;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.FileSystemMasterService;
import tachyon.thrift.InvalidPathException;

/**
 * A wrapper for the thrift client to interact with the file system master, used by tachyon clients.
 *
 * Since thrift clients are not thread safe, this class is a wrapper to provide thread safety, and
 * to provide retries.
 */
public final class FileSystemMasterClient extends MasterClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private FileSystemMasterService.Client mClient = null;

  /**
   * Creates a new file system master client.
   *
   * @param masterAddress the master address
   * @param executorService the executor service
   * @param tachyonConf the Tachyon configuration
   */
  public FileSystemMasterClient(InetSocketAddress masterAddress, ExecutorService executorService,
      TachyonConf tachyonConf) {
    super(masterAddress, executorService, tachyonConf);
  }

  @Override
  protected String getServiceName() {
    return Constants.FILE_SYSTEM_MASTER_SERVICE_NAME;
  }

  @Override
  protected void afterConnect() {
    mClient = new FileSystemMasterService.Client(mProtocol);
  }

  /**
   * @param path the path
   * @return the file id for the given path
   * @throws InvalidPathException if the given path is invalid
   * @throws IOException if an I/O error occurs
   */
  public synchronized long getFileId(final String path) throws IOException, InvalidPathException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Long>() {
        @Override
        public Long call() throws PropagateTException, TException {
          try {
            return mClient.getFileId(path);
          } catch (InvalidPathException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (InvalidPathException) e.getWrappedTException();
    }
  }

  /**
   * @param fileId the file id
   * @return the file info for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized FileInfo getFileInfo(final long fileId) throws IOException,
      FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<FileInfo>() {
        @Override
        public FileInfo call() throws PropagateTException, TException {
          try {
            return mClient.getFileInfo(fileId);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * @param fileId the file id
   * @return the list of file information for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized List<FileInfo> getFileInfoList(final long fileId) throws IOException,
      FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<List<FileInfo>>() {
        @Override
        public List<FileInfo> call() throws PropagateTException, TException {
          try {
            return mClient.getFileInfoList(fileId);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * @param fileId the file id
   * @param fileBlockIndex the file block index
   * @return the file block information
   * @throws FileDoesNotExistException if the file does not exist
   * @throws BlockInfoException if the block index is invalid
   * @throws IOException if an I/O error occurs
   */
  // TODO(calvin): Not sure if this is necessary.
  public synchronized FileBlockInfo getFileBlockInfo(final long fileId, final int fileBlockIndex)
      throws IOException, FileDoesNotExistException, BlockInfoException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<FileBlockInfo>() {
        @Override
        public FileBlockInfo call() throws PropagateTException, TException {
          try {
            return mClient.getFileBlockInfo(fileId, fileBlockIndex);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          } catch (BlockInfoException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      TException te = e.getWrappedTException();
      if (te instanceof FileDoesNotExistException) {
        throw (FileDoesNotExistException) te;
      }
      throw (BlockInfoException) te;
    }
  }

  /**
   * @param fileId the file id
   * @return the list of file block information for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  // TODO(calvin): Not sure if this is necessary.
  public synchronized List<FileBlockInfo> getFileBlockInfoList(final long fileId)
      throws IOException, FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<List<FileBlockInfo>>() {
        @Override
        public List<FileBlockInfo> call() throws PropagateTException, TException {
          try {
            return mClient.getFileBlockInfoList(fileId);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * @param fileId the file id
   * @return a new block id for the given file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs.
   */
  public synchronized long getNewBlockIdForFile(final long fileId) throws IOException,
      FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Long>() {
        @Override
        public Long call() throws PropagateTException, TException {
          try {
            return mClient.getNewBlockIdForFile(fileId);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * @return the under file system address
   * @throws IOException if an I/O error occurs
   */
  public synchronized String getUfsAddress() throws IOException {
    return retryRPC(new RpcCallable<String>() {
      @Override
      public String call() throws TException {
        return mClient.getUfsAddress();
      }
    });
  }

  /**
   * Creates a new file.
   *
   * @param path the file path
   * @param blockSizeBytes the file size
   * @param recursive whether parent directories should be created if not present yet
   * @return the file id
   * @throws InvalidPathException if the given path is invalid
   * @throws BlockInfoException if the block index is invalid
   * @throws FileAlreadyExistException if the file already exists
   * @throws IOException if an I/O error occurs
   */
  public synchronized long createFile(final String path, final long blockSizeBytes,
      final boolean recursive) throws IOException, BlockInfoException, InvalidPathException,
        FileAlreadyExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Long>() {
        @Override
        public Long call() throws PropagateTException, TException {
          try {
            return mClient.createFile(path, blockSizeBytes, recursive);
          } catch (BlockInfoException e) {
            throw new PropagateTException(e);
          } catch (InvalidPathException e) {
            throw new PropagateTException(e);
          } catch (FileAlreadyExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      TException te = e.getWrappedTException();
      if (te instanceof BlockInfoException) {
        throw (BlockInfoException) te;
      } else if (te instanceof InvalidPathException) {
        throw (InvalidPathException) te;
      }
      throw (FileAlreadyExistException) te;
    }
  }

  /**
   * Loads a file from the under file system.
   *
   * @param path the file path
   * @param ufsPath the under file system path
   * @param recursive whether parent directories should be loaded if not present yet
   * @return the file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized long loadFileInfoFromUfs(final String path, final String ufsPath,
      final boolean recursive) throws IOException, FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Long>() {
        @Override
        public Long call() throws PropagateTException, TException {
          try {
            return mClient.loadFileInfoFromUfs(path, ufsPath, recursive);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * Marks a file as completed.
   *
   * @param fileId the file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws BlockInfoException if the block index is invalid
   * @throws IOException if an I/O error occurs
   */
  public synchronized void completeFile(final long fileId) throws IOException,
      FileDoesNotExistException, BlockInfoException {
    try {
      retryRPC(new RpcCallableWithPropagateTException<Void>() {
        @Override
        public Void call() throws PropagateTException, TException {
          try {
            mClient.completeFile(fileId);
            return null;
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          } catch (BlockInfoException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      TException te = e.getWrappedTException();
      if (te instanceof FileDoesNotExistException) {
        throw (FileDoesNotExistException) te;
      }
      throw (BlockInfoException) te;
    }
  }

  /**
   * Deletes a file.
   *
   * @param fileId the file id
   * @param recursive whether to delete the file recursively (when it is a directory)
   * @return whether operation succeeded or not
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean deleteFile(final long fileId, final boolean recursive)
      throws IOException, FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Boolean>() {
        @Override
        public Boolean call() throws PropagateTException, TException {
          try {
            return mClient.deleteFile(fileId, recursive);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * Renames a file.
   *
   * @param fileId the file id
   * @param dstPath new file path
   * @return whether operation succeeded or not
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean renameFile(final long fileId, final String dstPath)
      throws IOException, FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Boolean>() {
        @Override
        public Boolean call() throws PropagateTException, TException {
          try {
            return mClient.renameFile(fileId, dstPath);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * Sets the "pinned" status for a file.
   *
   * @param fileId the file id
   * @param pinned the pinned status to use
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized void setPinned(final long fileId, final boolean pinned) throws IOException,
      FileDoesNotExistException {
    try {
      retryRPC(new RpcCallableWithPropagateTException<Void>() {
        @Override
        public Void call() throws PropagateTException, TException {
          try {
            mClient.setPinned(fileId, pinned);
            return null;
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * Creates a new directory.
   *
   * @param path the directory path
   * @param recursive whether parent directories should be created if they don't exist yet
   * @return whether operation succeeded or not
   * @throws InvalidPathException if the given path is invalid
   * @throws FileAlreadyExistException if the file already exists
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean createDirectory(final String path, final boolean recursive)
      throws IOException, FileAlreadyExistException, InvalidPathException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Boolean>() {
        @Override
        public Boolean call() throws PropagateTException, TException {
          try {
            return mClient.createDirectory(path, recursive);
          } catch (InvalidPathException e) {
            throw new PropagateTException(e);
          } catch (FileAlreadyExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      TException te = e.getWrappedTException();
      if (te instanceof InvalidPathException) {
        throw (InvalidPathException) te;
      }
      throw (FileAlreadyExistException) te;
    }
  }

  /**
   * Frees a file.
   *
   * @param fileId the file id
   * @param recursive whether free the file recursively (when it is a directory)
   * @return whether operation succeeded or not
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized boolean free(final long fileId, final boolean recursive) throws IOException,
      FileDoesNotExistException {
    try {
      return retryRPC(new RpcCallableWithPropagateTException<Boolean>() {
        @Override
        public Boolean call() throws PropagateTException, TException {
          try {
            return mClient.free(fileId, recursive);
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * Reports a lost file.
   *
   * @param fileId the file id
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized void reportLostFile(final long fileId) throws IOException,
      FileDoesNotExistException {
    try {
      retryRPC(new RpcCallableWithPropagateTException<Void>() {
        @Override
        public Void call() throws PropagateTException, TException {
          try {
            mClient.reportLostFile(fileId);
            return null;
          } catch (FileDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (FileDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * Requests files in a dependency.
   *
   * @param depId the dependency id
   * @throws DependencyDoesNotExistException if the dependency does not exist
   * @throws IOException if an I/O error occurs
   */
  public synchronized void requestFilesInDependency(final int depId) throws IOException,
      DependencyDoesNotExistException {
    try {
      retryRPC(new RpcCallableWithPropagateTException<Void>() {
        @Override
        public Void call() throws PropagateTException, TException {
          try {
            mClient.requestFilesInDependency(depId);
            return null;
          } catch (DependencyDoesNotExistException e) {
            throw new PropagateTException(e);
          }
        }
      });
    } catch (RpcCallableWithPropagateTException.PropagateTException e) {
      throw (DependencyDoesNotExistException) e.getWrappedTException();
    }
  }

  /**
   * Creates a dependency.
   *
   * Not implemented.
   *
   * @param parents the dependency parents
   * @param children the dependency children
   * @param commandPrefix the prefix of the dependency command
   * @param data the dependency data
   * @param comment a comment
   * @param framework the framework
   * @param frameworkVersion the framework version
   * @param dependencyType the dependency type
   * @param childrenBlockSizeByte the children block size (in bytes)
   * @return the dependency id
   * @throws IOException if an I/O error occurs
   */
  public synchronized int user_createDependency(List<String> parents, List<String> children,
      String commandPrefix, List<ByteBuffer> data, String comment, String framework,
      String frameworkVersion, int dependencyType, long childrenBlockSizeByte) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }

  /**
   * Gets dependency information for a dependency.
   *
   * Not implemented.
   *
   * @param dependencyId the dependency id
   * @return the dependency information
   * @throws IOException if an I/O error occurs
   */
  public synchronized DependencyInfo getDependencyInfo(int dependencyId) throws IOException {
    throw new UnsupportedOperationException("not implemented");
  }
}
