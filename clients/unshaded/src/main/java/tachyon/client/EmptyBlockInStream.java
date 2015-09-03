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

package tachyon.client;

import java.io.IOException;

import tachyon.conf.TachyonConf;

/**
 * <code>EmptyBlockInStream</code> is an <code>InStream</code> that can not read anything.
 */
public class EmptyBlockInStream extends InStream {
  /**
   * Creates a new <code>EmptyBlockInStream</code>.
   *
   * @param file the file the block belongs to
   * @param readType the InStream's read type
   * @param tachyonConf the TachyonConf instance for this stream.
   */
  EmptyBlockInStream(TachyonFile file, ReadType readType, TachyonConf tachyonConf) {
    super(file, readType, tachyonConf);
  }

  @Override
  public void close() throws IOException {}

  @Override
  public int read() throws IOException {
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return b.length == 0 ? 0 : -1;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (pos < 0) {
      throw new IOException("pos is negative: " + pos);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    return 0;
  }
}
