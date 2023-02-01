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
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileIncompleteException;
import alluxio.exception.OpenDirectoryException;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.OpenFilePOptions;
import alluxio.proto.dataserver.Protocol;
import alluxio.util.FileSystemOptionsUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
    if (!mMetadataCacheEnabled) {
      return mDelegatedFileSystem.getStatus(path, options);
    }
    try {
      return mDoraClient.getStatus(path.getPath(), options);
    } catch (RuntimeException ex) {
      LOG.debug("Dora client get status error", ex);
      return mDelegatedFileSystem.getStatus(path, options);
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
      LOG.debug("Dora client open file error", ex);
      return mDelegatedFileSystem.openFile(status, mergedOptions);
    }
  }
}
