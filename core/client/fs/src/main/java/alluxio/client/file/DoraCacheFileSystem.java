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
import alluxio.client.ReadType;
import alluxio.client.file.dora.DoraCacheClient;
import alluxio.client.file.dora.WorkerLocationPolicy;
import alluxio.client.file.ufs.UfsBaseFileSystem;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.ListStatusPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.grpc.RenamePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.FileSystemOptionsUtils;
import alluxio.util.io.PathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

/**
 * Dora Cache file system implementation.
 */
public class DoraCacheFileSystem extends DelegatingFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(DoraCacheFileSystem.class);
  public static final int DUMMY_MOUNT_ID = 0;
  private final DoraCacheClient mDoraClient;
  private final FileSystemContext mFsContext;
  private final boolean mMetadataCacheEnabled;

  /**
   * Wraps a file system instance to forward messages.
   *
   * @param fs the underlying file system
   * @param context
   */
  public DoraCacheFileSystem(FileSystem fs, FileSystemContext context) {
    super(fs);
    mDoraClient = new DoraCacheClient(context, new WorkerLocationPolicy(2000));
    mFsContext = context;
    mMetadataCacheEnabled = context.getClusterConf()
        .getBoolean(PropertyKey.DORA_CLIENT_METADATA_CACHE_ENABLED);
  }

  @Override
  public URIStatus getStatus(AlluxioURI path, GetStatusPOptions options)
      throws IOException, AlluxioException {
    AlluxioURI ufsFullPath = convertAlluxioPathToUFSPath(path);

    if (!mMetadataCacheEnabled) {
      return mDelegatedFileSystem.getStatus(ufsFullPath, options);
    }
    try {
      return mDoraClient.getStatus(path.getPath(), options);
    } catch (RuntimeException ex) {
      LOG.debug("Dora client get status error. Fall back to UFS.", ex);
      return mDelegatedFileSystem.getStatus(ufsFullPath, options);
    }
  }

  @Override
  public FileInStream openFile(AlluxioURI path, OpenFilePOptions options)
      throws IOException, AlluxioException {
    return openFile(getStatus(path), options);
  }

  @Override
  public FileInStream openFile(URIStatus status, OpenFilePOptions options)
      throws IOException, AlluxioException {
    AlluxioURI path = new AlluxioURI(status.getPath());
    if (status.isFolder()) {
      throw new OpenDirectoryException(path);
    }
    if (!status.isCompleted()) {
      throw new FileIncompleteException(path);
    }
    AlluxioConfiguration conf = mFsContext.getPathConf(path);
    OpenFilePOptions mergedOptions = FileSystemOptionsUtils.openFileDefaults(conf)
        .toBuilder().mergeFrom(options).build();
    try {
      Protocol.OpenUfsBlockOptions openUfsBlockOptions =
          Protocol.OpenUfsBlockOptions.newBuilder().setUfsPath(status.getUfsPath())
              .setOffsetInFile(0).setBlockSize(status.getLength())
              .setMaxUfsReadConcurrency(mergedOptions.getMaxUfsReadConcurrency())
              .setNoCache(!ReadType.fromProto(mergedOptions.getReadType()).isCache())
              .setMountId(DUMMY_MOUNT_ID)
              .build();
      return mDoraClient.getInStream(status, openUfsBlockOptions);
    } catch (RuntimeException ex) {
      LOG.debug("Dora client open file error. Fall back to UFS.", ex);
      return mDelegatedFileSystem.openFile(status, mergedOptions);
    }
  }

  @Override
  public List<URIStatus> listStatus(AlluxioURI path, ListStatusPOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    AlluxioURI ufsFullPath = convertAlluxioPathToUFSPath(path);
    return mDelegatedFileSystem.listStatus(ufsFullPath, options);
  }

  @Override
  public FileOutStream createFile(AlluxioURI path, CreateFilePOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    AlluxioURI ufsFullPath = convertAlluxioPathToUFSPath(path);
    LOG.warn("Dora Client does not support create/write. This is only for test.");
    return mDelegatedFileSystem.createFile(ufsFullPath, options);
  }

  @Override
  public void createDirectory(AlluxioURI path, CreateDirectoryPOptions options)
      throws FileAlreadyExistsException, InvalidPathException, IOException, AlluxioException {
    AlluxioURI ufsFullPath = convertAlluxioPathToUFSPath(path);
    LOG.warn("Dora Client does not support create/write. This is only for test.");

    mDelegatedFileSystem.createDirectory(ufsFullPath, options);
  }

  @Override
  public void rename(AlluxioURI src, AlluxioURI dst, RenamePOptions options)
      throws FileDoesNotExistException, IOException, AlluxioException {
    AlluxioURI srcUfsFullPath = convertAlluxioPathToUFSPath(src);
    AlluxioURI dstUfsFullPath = convertAlluxioPathToUFSPath(dst);
    LOG.warn("Dora Client does not support create/write. This is only for test.");

    mDelegatedFileSystem.rename(srcUfsFullPath, dstUfsFullPath, options);
  }

  @Override
  public void iterateStatus(AlluxioURI path, ListStatusPOptions options,
                            Consumer<? super URIStatus> action)
      throws FileDoesNotExistException, IOException, AlluxioException {
    AlluxioURI ufsFullPath = convertAlluxioPathToUFSPath(path);

    mDelegatedFileSystem.iterateStatus(ufsFullPath, options, action);
  }

  /**
   * Converts the Alluxio based path to UfsBaseFileSystem based path.
   *
   * UfsBaseFileSystem expects absolute/full file path.
   *
   * @param alluxioPath Alluxio based path
   * @return UfsBaseFileSystem based full path
   */
  private AlluxioURI convertAlluxioPathToUFSPath(AlluxioURI alluxioPath) {
    UfsBaseFileSystem under = (UfsBaseFileSystem) mDelegatedFileSystem;
    String ufsFullPath = PathUtils.concatPath(under.getRootUFS(), alluxioPath.getPath());
    return new AlluxioURI(ufsFullPath);
  }
}
