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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.file.options.CreateOptions;
import tachyon.client.file.options.DeleteOptions;
import tachyon.client.file.options.FreeOptions;
import tachyon.client.file.options.GetInfoOptions;
import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.ListStatusOptions;
import tachyon.client.file.options.LoadMetadataOptions;
import tachyon.client.file.options.MkdirOptions;
import tachyon.client.file.options.MountOptions;
import tachyon.client.file.options.OpenOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.client.file.options.RenameOptions;
import tachyon.client.file.options.SetStateOptions;
import tachyon.client.file.options.UnmountOptions;
import tachyon.client.lineage.TachyonLineageFileSystem;
import tachyon.exception.ExceptionMessage;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

/**
 * A {@link TachyonFileSystem} implementation including convenience methods as well as a streaming
 * API to read and write files. This class does not access the master client directly but goes
 * through the implementations provided in {@link AbstractTachyonFileSystem}. The create API for
 * creating files is not supported by this TachyonFileSystem because the files should only be
 * written once, thus {@link #getOutStream(TachyonURI)} is sufficient for creating and writing to a
 * file.
 */
@PublicApi
public class TachyonFileSystem extends AbstractTachyonFileSystem {
  private static TachyonFileSystem sTachyonFileSystem;

  /**
   * Factory for the {@link TachyonFileSystem}.
   */
  public static final class TachyonFileSystemFactory {
    private TachyonFileSystemFactory() {} // to prevent initialization

    /**
     * @return the appropriate file system depending on the configuration for the lineage
     */
    public static synchronized TachyonFileSystem get() {
      boolean enableLineage = ClientContext.getConf().getBoolean(Constants.USER_LINEAGE_ENABLED);
      return enableLineage ? TachyonLineageFileSystem.get() : TachyonFileSystem.get();
    }
  }

  static synchronized TachyonFileSystem get() {
    if (sTachyonFileSystem == null) {
      sTachyonFileSystem = new TachyonFileSystem();
    }
    return sTachyonFileSystem;
  }

  protected TachyonFileSystem() {
    super();
  }

  /**
   * Convenience method for {@link #create(TachyonURI, CreateOptions)} with default options.
   *
   * @param path the path of the file to create in Tachyon space
   * @return the {@link TachyonFile} instance that identifies the newly created file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  public TachyonFile create(TachyonURI path)
      throws IOException, TachyonException, FileAlreadyExistsException, InvalidPathException {
    return create(path, CreateOptions.defaults());
  }

  /**
   * Convenience method for {@link #delete(TachyonFile, DeleteOptions)} with default options.
   *
   * @param file the handler of the file to delete
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileDoesNotExistException if the given file does not exist
   */
  public void delete(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    delete(file, DeleteOptions.defaults());
  }

  /**
   * Convenience method for {@link #free(TachyonFile, FreeOptions)} with default options.
   *
   * @param file the handler for the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileDoesNotExistException if the given file does not exist
   */
  public void free(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    free(file, FreeOptions.defaults());
  }

  /**
   * Convenience method for {@link TachyonFileSystemCore#getInfo(TachyonFile, GetInfoOptions)} with
   * default options.
   *
   * @param file the handler for the file
   * @return the {@link FileInfo} of the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   */
  public FileInfo getInfo(TachyonFile file)
      throws FileDoesNotExistException, IOException, TachyonException {
    return getInfo(file, GetInfoOptions.defaults());
  }

  /**
   * Convenience method for {@link #getInStream(TachyonFile, InStreamOptions)} with default options.
   *
   * @param file the handler for the file to read
   * @return an input stream to read the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileDoesNotExistException if the given file does not exist
   */
  public FileInStream getInStream(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    return getInStream(file, InStreamOptions.defaults());
  }

  /**
   * Gets a {@link FileInStream} for the specified file. The stream's settings can be customized by
   * setting the options parameter. The caller should close the stream after finishing the
   * operations on it.
   *
   * @param file the handler for the file to read
   * @param options the set of options specific to this operation
   * @return an input stream to read the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileDoesNotExistException if the given file does not exist
   */
  public FileInStream getInStream(TachyonFile file, InStreamOptions options)
      throws IOException, TachyonException, FileDoesNotExistException {
    FileInfo info = getInfo(file, GetInfoOptions.defaults());
    if (info.isIsFolder()) {
      throw new FileNotFoundException(
          ExceptionMessage.CANNOT_READ_DIRECTORY.getMessage(info.getName()));
    }
    return new FileInStream(info, options);
  }

  /**
   * Convenience method for {@link #getOutStream(TachyonURI, OutStreamOptions)} with default
   * options.
   *
   * @param path the Tachyon path of the file
   * @return an output stream to write the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  public FileOutStream getOutStream(TachyonURI path)
      throws IOException, TachyonException, FileAlreadyExistsException, InvalidPathException {
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
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  public FileOutStream getOutStream(TachyonURI path, OutStreamOptions options)
      throws IOException, TachyonException, FileAlreadyExistsException, InvalidPathException {
    CreateOptions createOptions =
        new CreateOptions.Builder(ClientContext.getConf())
            .setBlockSizeBytes(options.getBlockSizeBytes())
            .setRecursive(true)
            .setTtl(options.getTtl())
            .setUnderStorageType(options.getUnderStorageType())
            .build();
    TachyonFile tFile = create(path, createOptions);
    try {
      return new FileOutStream(tFile.getFileId(), options);
    } catch (IOException e) {
      // Delete the file if it still exists
      TachyonFile file = openIfExists(path);
      if (file != null) {
        delete(file);
      }
      throw e;
    }
  }

  /**
   * Alternative way to get a {@link FileOutStream} to a file that has already been created. This
   * should not be used.
   *
   * @param fileId the id of the file
   * @param options the set of options specific to this operation
   * @return an output stream to write the file
   * @throws IOException if a non-Tachyon exception occurs
   *
   * @see #getOutStream(TachyonURI path, OutStreamOptions options)
   * @deprecated Deprecated in version v0.8 and will be removed in v0.9.
   */
  // TODO(calvin): We should remove this when the TachyonFS code is fully deprecated.
  @Deprecated
  public FileOutStream getOutStream(long fileId, OutStreamOptions options) throws IOException {
    return new FileOutStream(fileId, options);
  }

  /**
   * Convenience method for {@link #listStatus(TachyonFile, ListStatusOptions)} with default
   * options.
   *
   * @param file the handler for the file
   * @return a list of {@link FileInfo}s representing the files which are children of the given file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileDoesNotExistException if the given file does not exist
   */
  public List<FileInfo> listStatus(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    return listStatus(file, ListStatusOptions.defaults());
  }

  /**
   * Convenience method for {@link #loadMetadata(TachyonURI, LoadMetadataOptions)} with default
   * options.
   *
   * @param path the path for which to load metadata from UFS
   * @return the {@link TachyonFile} instance identifying the resulting file in Tachyon
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileDoesNotExistException if the given file does not exist
   */
  public TachyonFile loadMetadata(TachyonURI path)
      throws IOException, TachyonException, FileDoesNotExistException {
    return loadMetadata(path, LoadMetadataOptions.defaults());
  }

  /**
   * Convenience method for {@link #mkdir(TachyonURI, MkdirOptions)} with default options.
   *
   * @param path the handler for the file
   * @return true if the directory is created successfully or already existing, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException  if an unexpected Tachyon exception is thrown
   * @throws FileAlreadyExistsException if there is already a file at the given path
   * @throws InvalidPathException if the path is invalid
   */
  public boolean mkdir(TachyonURI path)
      throws IOException, TachyonException, FileAlreadyExistsException, InvalidPathException {
    return mkdir(path, MkdirOptions.defaults());
  }

  /**
   * Convenience method for {@link #mount(TachyonURI, TachyonURI, MountOptions)} with default
   * options.
   *
   * @param tachyonPath a Tachyon path
   * @param ufsPath a UFS path
   * @return true if the UFS subtree was mounted successfully, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public boolean mount(TachyonURI tachyonPath, TachyonURI ufsPath)
      throws IOException, TachyonException {
    return mount(tachyonPath, ufsPath, MountOptions.defaults());
  }

  /**
   * Convenience method for {@link #open(TachyonURI, OpenOptions)} with default options.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @return a {@link TachyonFile} which acts as a file handler for the path
   * @throws IOException if a non-Tachyon exception occurs
   * @throws InvalidPathException if there is no file at the given path
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   */
  public TachyonFile open(TachyonURI path)
      throws IOException, InvalidPathException, TachyonException {
    return open(path, OpenOptions.defaults());
  }

  /**
   * Convenience method for {@link #openIfExists(TachyonURI, OpenOptions)} with default options.
   *
   * @param path the path of the file, this should be in Tachyon space
   * @return a {@link TachyonFile} which acts as a file handler for the path or null if there is no
   *         file at the given path
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   */
  public TachyonFile openIfExists(TachyonURI path) throws IOException, TachyonException {
    return openIfExists(path, OpenOptions.defaults());
  }

  /**
   * Convenience method for {@link #rename(TachyonFile, TachyonURI, RenameOptions)} with default
   * options.
   *
   * @param src the file handler for the source file
   * @param dst the path of the destination file, this path should not exist
   * @return true if successful, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   * @throws FileDoesNotExistException if the given file does not exist
   */
  public boolean rename(TachyonFile src, TachyonURI dst)
      throws IOException, TachyonException, FileDoesNotExistException {
    return rename(src, dst, RenameOptions.defaults());
  }

  /**
   * Convenience method for {@link #setState(TachyonFile, SetStateOptions)} with default options.
   *
   * @param file the file handler for the file
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if an unexpected Tachyon exception is thrown
   */
  public void setState(TachyonFile file) throws IOException, TachyonException {
    setState(file, SetStateOptions.defaults());
  }

  /**
   * Convenience method for {@link #unmount(TachyonURI, UnmountOptions)} with default options.
   *
   * @param tachyonPath a Tachyon path
   * @return true if the UFS subtree was unmounted successfully, false otherwise
   * @throws IOException if a non-Tachyon exception occurs
   * @throws TachyonException if a Tachyon exception occurs
   */
  public boolean unmount(TachyonURI tachyonPath) throws IOException, TachyonException {
    return unmount(tachyonPath, UnmountOptions.defaults());
  }
}
