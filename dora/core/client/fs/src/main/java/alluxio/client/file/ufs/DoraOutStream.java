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

package alluxio.client.file.ufs;

import alluxio.AlluxioURI;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.dora.DoraCacheClient;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.exception.runtime.PermissionDeniedRuntimeException;
import alluxio.exception.status.PermissionDeniedException;
import alluxio.grpc.CompleteFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.util.CommonUtils;

import com.google.common.base.Preconditions;
import com.google.common.io.Closer;

import java.io.IOException;

/**
 * A Dora output stream.
 */
public class DoraOutStream extends FileOutStream {

  /** Used to manage closeable resources. */
  private final Closer mCloser;
  private final FileSystemContext mContext;
  private final OutStreamOptions mOptions;
  private boolean mClosed;
  private final AlluxioURI mUri;
  private final DoraCacheClient mDoraClient;

  private final String mUuid;

  /**
   * Creates a new file output stream.
   *
   * @param path the file path
   * @param options the client options
   * @param context the file system context
   * @param uuid the uuid of its file open file
   * @param doraClient the client saved to do close()
   */
  public DoraOutStream(AlluxioURI path, OutStreamOptions options, FileSystemContext context,
                       String uuid, DoraCacheClient doraClient)
      throws IOException {
    mCloser = Closer.create();
    // Acquire a resource to block FileSystemContext reinitialization, this needs to be done before
    // using mContext.
    // The resource will be released in close().
    mContext = context;
    mCloser.register(mContext.blockReinit());
    mDoraClient = doraClient;
    try {
      mUri = Preconditions.checkNotNull(path, "path");
      mOptions = options;
      mClosed = false;
      mBytesWritten = 0;
      mUuid = uuid;
    } catch (Throwable t) {
      throw CommonUtils.closeAndRethrow(mCloser, t);
    }
  }

  @Override
  public void write(int b) throws IOException {
     // Add write implementation here. mOptions might be used.

    // This is just to eliminate the URF_UNREAD_FIELD error.
    // Use it or remove it please.
    mOptions.toString();
  }

  @Override
  public void close() {
    if (!mClosed) {
      CompleteFilePOptions options = CompleteFilePOptions.newBuilder()
          .setUfsLength(12345) // fill actual length of this file please.
          .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().build())
          .setContentHash("HASH-256") // compute hash here
          .build();
      mClosed = true;
      try {
        mDoraClient.completeFile(mUri.toString(), options, mUuid);
      } catch (PermissionDeniedException e) {
        throw new PermissionDeniedRuntimeException(e);
      }
    }
  }
}
