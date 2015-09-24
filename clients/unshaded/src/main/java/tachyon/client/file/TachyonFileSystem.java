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

import java.io.IOException;

import com.google.common.base.Preconditions;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.FileSystemMasterClient;
import tachyon.client.options.CreateOptions;
import tachyon.client.options.DeleteOptions;
import tachyon.client.options.FreeOptions;
import tachyon.client.options.InStreamOptions;
import tachyon.client.options.LoadOptions;
import tachyon.client.options.MkdirOptions;
import tachyon.client.options.OutStreamOptions;
import tachyon.client.options.SetStateOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.exception.TachyonExceptionType;
import tachyon.thrift.BlockInfoException;
import tachyon.thrift.DependencyDoesNotExistException;
import tachyon.thrift.FileAlreadyExistException;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.FileInfo;
import tachyon.thrift.InvalidPathException;

/**
 * A TachyonFileSystem implementation including convenience methods as well as a streaming API to
 * read and write files. This class does not access the master client directly but goes through the
 * implementations provided in {@link AbstractTachyonFileSystem}. The create api for creating files
 * is not supported by this TachyonFileSystem because the files should only be written once, thus
 * getOutStream is sufficient for creating and writing to a file.
 */
@PublicApi
public class TachyonFileSystem extends AbstractTachyonFileSystem {
  private static TachyonFileSystem sTachyonFileSystem;

  public static final boolean RECURSIVE = true;

  public static synchronized TachyonFileSystem get() {
    if (sTachyonFileSystem == null) {
      sTachyonFileSystem = new TachyonFileSystem();
    }
    return sTachyonFileSystem;
  }

  private TachyonFileSystem() {
    super();
  }

  @Override
  public long create(TachyonURI path) throws IOException, TachyonException {
    return create(path, CreateOptions.defaults());
  }

  @Override
  public long create(TachyonURI path, CreateOptions options) throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long fileId =
          masterClient.createFile(path.getPath(), options.getBlockSize(), options.isRecursive());
      return fileId;
    } catch (BlockInfoException e) {
      throw new TachyonException(e.getMessage(), TachyonExceptionType.FILE_ALREADY_EXISTS);
    } catch (FileAlreadyExistException e) {
      throw new TachyonException(e.getMessage(), TachyonExceptionType.FILE_ALREADY_EXISTS);
    } catch (InvalidPathException e) {
      throw new TachyonException(e.getMessage(), TachyonExceptionType.INVALID_PATH);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public void delete(TachyonFile file) throws IOException, TachyonException {
    delete(file, DeleteOptions.defaults());
  }

  @Override
  public void free(TachyonFile file) throws IOException, TachyonException {
    free(file, FreeOptions.defaults());
  }

  /**
   * Convenience method for {@link #getInStream(TachyonFile, InStreamOptions)} with default
   * options.
   *
   * @param file the handler for the file to read
   * @return an input stream to read the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public FileInStream getInStream(TachyonFile file) throws IOException, TachyonException {
    return getInStream(file, InStreamOptions.defaults());
  }

  /**
   * Gets a {@link FileInStream} for the specified file. The stream's settings can be customized by
   * setting the options parameter. The caller should close the stream after finishing the
   * operations on it.
   *
   * @param file the handler for the file to read
   * @param options the set of options specific to this operation.
   * @return an input stream to read the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public FileInStream getInStream(TachyonFile file, InStreamOptions options) throws IOException,
      TachyonException {
    FileInfo info = getInfo(file);
    Preconditions.checkState(!info.isIsFolder(), "Cannot read from a folder");
    return new FileInStream(info, options);
  }

  /**
   * Convenience method for {@link #getOutStream(TachyonURI, OutStreamOptions)} with default client
   * options.
   *
   * @param path the Tachyon path of the file
   * @return an output stream to write the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public FileOutStream getOutStream(TachyonURI path) throws IOException, TachyonException {
    return getOutStream(path, OutStreamOptions.defaults());
  }

  /**
   * Creates a file and gets the {@link FileOutStream} for the specified file. If the parent
   * directories do not exist, they will be created. This should only be called to write a file that
   * does not exist. Once close is called on the output stream, the file will be completed. Append
   * or update of a completed file is currently not supported.
   *
   * @param path the Tachyon path of the file
   * @param options the set of options specific to this operation
   * @return an output stream to write the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public FileOutStream getOutStream(TachyonURI path, OutStreamOptions options) throws IOException,
      TachyonException {
    CreateOptions createOptions =
        (new CreateOptions.Builder(new TachyonConf())).setBlockSize(options.getBlockSize())
            .setRecursive(true).build();
    long fileId = create(path, createOptions);
    return new FileOutStream(fileId, options);
  }

  /**
   * Alternative way to get a FileOutStream to a file that has already been created. This should not
   * be used. Deprecated in version v0.8 and will be removed in v0.9.
   *
   * @see #getOutStream(TachyonURI path, OutStreamOptions options)
   */
  // TODO(calvin): We should remove this when the TachyonFS code is fully deprecated.
  @Deprecated
  public FileOutStream getOutStream(long fileId, OutStreamOptions options) throws IOException {
    return new FileOutStream(fileId, options);
  }

  @Override
  public long load(TachyonURI path, TachyonURI ufsPath) throws IOException, TachyonException {
    return load(path, ufsPath, LoadOptions.defaults());
  }

  @Override
  public boolean mkdir(TachyonURI path) throws IOException, TachyonException {
    return mkdir(path, MkdirOptions.defaults());
  }

  // TODO: Move this to lineage client
  public void reportLostFile(TachyonFile file) throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.reportLostFile(file.getFileId());
    } catch (FileDoesNotExistException e) {
      throw new TachyonException(e.getMessage(), TachyonExceptionType.FILE_DOES_NOT_EXIST);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  // TODO: Move this to lineage client
  public void requestFilesInDependency(int depId) throws IOException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.requestFilesInDependency(depId);
    } catch (DependencyDoesNotExistException e) {
      throw new TachyonException(e.getMessage(), TachyonExceptionType.DEPENDENCY_DOES_NOT_EXIST);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }


  @Override
  public void setState(TachyonFile file) throws IOException, TachyonException {
    setState(file, SetStateOptions.defaults());
  }
}
