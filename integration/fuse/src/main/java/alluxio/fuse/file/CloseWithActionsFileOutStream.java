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

package alluxio.fuse.file;

import alluxio.client.file.FileOutStream;

import java.io.IOException;

/**
 * A wrapper class for FileOutStream that performs some operations after the FileInputStream is
 * closed.
 */
public class CloseWithActionsFileOutStream extends FileOutStream {

  private final FileOutStream mFileOutStream;
  private final Runnable[] mActions;

  /**
   *
   * @param fileOutStream
   * @param actions
   */
  public CloseWithActionsFileOutStream(FileOutStream fileOutStream, Runnable... actions) {
    mFileOutStream = fileOutStream;
    mActions = actions;
  }

  @Override
  public long getBytesWritten() {
    return mFileOutStream.getBytesWritten();
  }

  @Override
  public void cancel() throws IOException {
    mFileOutStream.cancel();
  }

  @Override
  public void write(int b) throws IOException {
    mFileOutStream.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    mFileOutStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    mFileOutStream.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    mFileOutStream.flush();
  }

  @Override
  public void close() throws IOException {
    mFileOutStream.close();
    if (mActions == null) {
      return;
    }
    for (Runnable action : mActions) {
      action.run();
    }
  }
}

