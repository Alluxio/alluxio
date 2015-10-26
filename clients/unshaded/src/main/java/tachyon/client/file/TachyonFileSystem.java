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
import java.util.List;

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.ClientContext;
import tachyon.client.FileSystemMasterClient;
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
import tachyon.conf.TachyonConf;
import tachyon.exception.FileAlreadyExistsException;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.InvalidPathException;
import tachyon.exception.PreconditionMessage;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;

/**
 * A TachyonFileSystem implementation including convenience methods as well as a streaming API to
 * read and write files. This class does not access the master client directly but goes through the
 * implementations provided in {@link AbstractTachyonFileSystem}. The create API for creating files
 * is not supported by this TachyonFileSystem because the files should only be written once, thus
 * getOutStream is sufficient for creating and writing to a file.
 */
@PublicApi
public class TachyonFileSystem extends AbstractTachyonFileSystem {
  private static TachyonFileSystem sTachyonFileSystem;

  private final TachyonConf mTachyonConf;

  public static class TachyonFileSystemFactory {
    public static TachyonFileSystem get() {
      return get(ClientContext.getConf());
    }

    public static synchronized TachyonFileSystem get(TachyonConf conf) {
      if (sTachyonFileSystem == null) {
        boolean enableLineage = conf.getBoolean(Constants.USER_LINEAGE_ENABLED);
        sTachyonFileSystem =
            enableLineage ? TachyonLineageFileSystem.get(conf) : new TachyonFileSystem(conf);
      }
      return sTachyonFileSystem;
    }
  }

  protected TachyonFileSystem(TachyonConf conf) {
    super();
    mTachyonConf = conf;
  }

  /**
   * Convenience method for {@link #create(TachyonURI, CreateOptions)} with default options.
   */
  public TachyonFile create(TachyonURI path)
      throws IOException, TachyonException, FileAlreadyExistsException, InvalidPathException {
    return create(path, new CreateOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #delete(TachyonFile, DeleteOptions)} with default options.
   */
  public void delete(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    delete(file, new DeleteOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #free(TachyonFile, FreeOptions)} with default options.
   */
  public void free(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    free(file, new FreeOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link TachyonFileSystemCore#getInfo(TachyonFile, GetInfoOptions)} with
   * default options.
   */
  public FileInfo getInfo(TachyonFile file)
      throws FileDoesNotExistException, IOException, TachyonException {
    return getInfo(file, new GetInfoOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #getInStream(TachyonFile, InStreamOptions)} with default options.
   */
  public FileInStream getInStream(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    return getInStream(file, new InStreamOptions.Builder(mTachyonConf).build());
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
    FileInfo info = getInfo(file, new GetInfoOptions.Builder(mTachyonConf).build());
    Preconditions.checkState(!info.isIsFolder(), PreconditionMessage.CANNOT_READ_FOLDER);
    return new FileInStream(info, options);
  }

  /**
   * Convenience method for {@link #getOutStream(TachyonURI, OutStreamOptions)} with default
   * options.
   */
  public FileOutStream getOutStream(TachyonURI path)
      throws IOException, TachyonException, FileAlreadyExistsException, InvalidPathException {
    return getOutStream(path, new OutStreamOptions.Builder(mTachyonConf).build());
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
        new CreateOptions.Builder(mTachyonConf)
            .setBlockSizeBytes(options.getBlockSizeBytes())
            .setRecursive(true)
            .setTTL(options.getTTL())
            .setUnderStorageType(options.getUnderStorageType())
            .build();
    TachyonFile tFile = create(path, createOptions);
    long fileId = tFile.getFileId();
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

  /**
   * Convenience method for {@link #listStatus(TachyonFile, ListStatusOptions)} with default
   * options.
   */
  public List<FileInfo> listStatus(TachyonFile file)
      throws IOException, TachyonException, FileDoesNotExistException {
    return listStatus(file, new ListStatusOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #loadMetadata(TachyonURI, LoadMetadataOptions)} with default
   * options.
   */
  public TachyonFile loadMetadata(TachyonURI path)
      throws IOException, TachyonException, FileDoesNotExistException {
    return loadMetadata(path, new LoadMetadataOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #mkdir(TachyonURI, MkdirOptions)} with default options.
   */
  public boolean mkdir(TachyonURI path)
      throws IOException, TachyonException, FileAlreadyExistsException, InvalidPathException {
    return mkdir(path, new MkdirOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #mount(TachyonURI, TachyonURI, MountOptions)} with default
   * options.
   */
  public boolean mount(TachyonURI tachyonPath, TachyonURI ufsPath)
      throws IOException, TachyonException {
    return mount(tachyonPath, ufsPath, new MountOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #open(TachyonURI, OpenOptions)} with default options.
   */
  public TachyonFile open(TachyonURI path)
      throws IOException, InvalidPathException, TachyonException {
    return open(path, new OpenOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #openIfExists(TachyonURI, OpenOptions)} with default options.
   */
  public TachyonFile openIfExists(TachyonURI path) throws IOException, TachyonException {
    return openIfExists(path, new OpenOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #rename(TachyonFile, TachyonURI, RenameOptions)} with default
   * options.
   */
  public boolean rename(TachyonFile src, TachyonURI dst)
      throws IOException, TachyonException, FileDoesNotExistException {
    return rename(src, dst, new RenameOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #setState(TachyonFile, SetStateOptions)} with default options.
   */
  public void setState(TachyonFile file) throws IOException, TachyonException {
    setState(file, new SetStateOptions.Builder(mTachyonConf).build());
  }

  /**
   * Convenience method for {@link #unmount(TachyonURI, UnmountOptions)} with default options.
   */
  public boolean unmount(TachyonURI tachyonPath) throws IOException, TachyonException {
    return unmount(tachyonPath, new UnmountOptions.Builder(mTachyonConf).build());
  }

  // TODO: Move this to lineage client
  public void reportLostFile(TachyonFile file)
      throws IOException, FileDoesNotExistException, TachyonException {
    FileSystemMasterClient masterClient = mContext.acquireMasterClient();
    try {
      masterClient.reportLostFile(file.getFileId());
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }
}
