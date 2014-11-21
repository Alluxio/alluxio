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

import java.io.InputStream;
import java.io.IOException;

/**
 * <code>InStream</code> is the base input stream class for TachyonFile streaming input methods. It
 * can only be gotten by calling the methods in <code>tachyon.client.TachyonFile</code>, but can not
 * be initialized by the client code.
 */
public abstract class InStream extends InputStream {
  protected final TachyonFile mFile;
  protected final TachyonFS mTachyonFS;
  protected final ReadType mReadType;

  /**
   * @param file the input file of the InStream
   * @param readType the InStream's read type
   */
  InStream(TachyonFile file, ReadType readType) {
    mFile = file;
    mTachyonFS = mFile.mTachyonFS;
    mReadType = readType;
  }

  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract int read() throws IOException;

  @Override
  public abstract int read(byte[] b) throws IOException;

  @Override
  public abstract int read(byte[] b, int off, int len) throws IOException;

  /**
   * Sets the stream pointer offset, measured from the beginning of this stream, at which the next
   * read or write occurs. The offset may be set beyond the end of the stream.
   * 
   * @param pos the offset position, measured in bytes from the beginning of the InStream, at which
   *        to set the stream pointer.
   * @throws IOException if pos is less than 0 or if an I/O error occurs.
   */
  public abstract void seek(long pos) throws IOException;

  @Override
  public abstract long skip(long n) throws IOException;
}
