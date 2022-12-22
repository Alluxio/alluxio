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

package alluxio.file;

import alluxio.Seekable;

import com.google.common.base.Preconditions;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A class optimizes seekable input streams by providing buffering.
 */
public class SeekableBufferedInputStream extends BufferedInputStream implements Seekable {

  /**
   * Constructor.
   * @param in input stream implements seekable
   * @param size the buffer size
   */
  public SeekableBufferedInputStream(InputStream in, int size) {
    super(in, size);
    Preconditions.checkArgument(in instanceof Seekable,
        "Input stream must implement Seeakble");
  }

  @Override
  public long getPos() throws IOException {
    return ((Seekable) in).getPos() - (count - pos);
  }

  @Override
  public long skip(long n) throws IOException {
    if (n <= 0) {
      return 0;
    }

    seek(getPos() + n);
    return n;
  }

  @Override
  public void seek(long newPosition) throws IOException {
    if (newPosition < 0) {
      return;
    }
    // optimize: check if the pos is in the buffer
    long end = ((Seekable) in).getPos();
    long start = end - count;
    if (start <= newPosition && newPosition < end) {
      pos = (int) (newPosition - start);
      return;
    }

    // invalidate buffer
    pos = 0;
    count = 0;

    ((Seekable) in).seek(newPosition);
  }
}
