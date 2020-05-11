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

package alluxio.underfs.hdfs;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class PreadSeekableStream extends FilterInputStream implements Seekable, PositionedReadable {
  protected PreadSeekableStream(InputStream in) {
    super(in);
  }

  @Override
  public int read(long position, byte[] bytes, int offset, int length) throws IOException {
    return ((FSDataInputStream) in).read(position, bytes, offset, length);
  }

  @Override
  public void readFully(long position, byte[] bytes, int offset, int length) throws IOException {
    ((FSDataInputStream) in).readFully(position, bytes, offset, length);
  }

  @Override
  public void readFully(long position, byte[] bytes) throws IOException {
    ((FSDataInputStream) in).readFully(position, bytes);
  }

  @Override
  public void seek(long position) throws IOException {
    ((FSDataInputStream) in).seek(position);
  }

  @Override
  public long getPos() throws IOException {
    return ((FSDataInputStream) in).getPos();
  }

  @Override
  public boolean seekToNewSource(long position) throws IOException {
    return ((FSDataInputStream) in).seekToNewSource(position);
  }
}
