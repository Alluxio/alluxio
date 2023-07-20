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

import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Open File Handle in Dora for write request.
 */
public class OpenFileHandle {
  private final String   mPath;
  private final FileInfo mInfo;
  private final UUID     mUUID;
  private long           mPos;
  private long           mLastAccessTimeMs;
  private OutputStream   mUfsOutStream; //outstream from UFS
  private boolean        mClosed;

  private final CreateFilePOptions mOptions;

  /**
   * Construct a new open file handle.
   * @param path the path of the file
   * @param info the FileInfo of this file
   * @param options the options of create
   * @param ufsOutStream the UFS output stream of this file
   */
  public OpenFileHandle(String path, FileInfo info, CreateFilePOptions options,
                        @Nullable OutputStream ufsOutStream) {
    mPath = path;
    mInfo = info;
    // TODO(Hua): The operation of generating UUID is SLOW. We can replace it in other way.
    mUUID = UUID.randomUUID();
    mUfsOutStream = ufsOutStream;
    mPos = 0L;
    mLastAccessTimeMs = System.currentTimeMillis();
    mClosed = false;
    mOptions = options;
  }

  /**
   * Get UUID.
   * @return the UUID of this handle
   */
  public UUID getUUID() {
    return mUUID;
  }

  /**
   * Get last accessed time.
   * @return the last accessed time of this handle
   */
  public long getLastAccessTimeMs() {
    return mLastAccessTimeMs;
  }

  /**
   *  Get path of this handle.
   * @return path of this handle
   */
  public String getPath() {
    return mPath;
  }

  /**
   * Get info of this handle.
   * @return info of this handle
   */
  public FileInfo getInfo() {
    return mInfo;
  }

  /**
   * Get write position of the out stream in this handle.
   * @return write position of the out stream in this handle
   */
  public long getPos() {
    return mPos;
  }

  /**
   * Get UFS out stream of this handle.
   * @return UFS out stream of this handle
   */
  public OutputStream getOutStream() {
    return mUfsOutStream;
  }

  /**
   * Get Alluxio create file options.
   * @return the CreateFilePOptions of this operation
   */
  public CreateFilePOptions getOptions() {
    return mOptions;
  }

  /**
   * Check if this handle is already closed.
   * @return true if this handle is close, false otherwise
   */
  public boolean isClosed() {
    return mClosed;
  }

  /**
   * Close this handle.
   */
  public void close() {
    mClosed = true;
    if (mUfsOutStream != null) {
      try {
        mUfsOutStream.close();
        mUfsOutStream = null;
      } catch (IOException e) {
        //Ignored
      }
    }
  }
}
