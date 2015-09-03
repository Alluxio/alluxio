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
import java.io.OutputStream;

import tachyon.conf.TachyonConf;

/**
 * <code>OutStream</code> is the base class of output stream for TachyonFile streaming output. To
 * get an instance of this class, one should call the method <code>getOutStream</code> of
 * <code>tachyon.client.TachyonFile</code>, rather than constructing a new instance directly in the
 * client code.
 */
public abstract class OutStream extends OutputStream {
  protected final TachyonFile mFile;
  protected final TachyonFS mTachyonFS;
  protected final WriteType mWriteType;
  protected final TachyonConf mTachyonConf;

  /**
   * @param file the output file of the OutStream
   * @param writeType the OutStream's write type
   */
  OutStream(TachyonFile file, WriteType writeType, TachyonConf tachyonConf) {
    mFile = file;
    mTachyonFS = mFile.mTachyonFS;
    mWriteType = writeType;
    mTachyonConf = tachyonConf;
  }

  /**
   * Cancel the write operations to the OutStream.
   *
   * @throws IOException when the operation fails
   */
  public abstract void cancel() throws IOException;

  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract void flush() throws IOException;

  @Override
  public abstract void write(byte[] b) throws IOException;

  @Override
  public abstract void write(byte[] b, int off, int len) throws IOException;

  @Override
  public abstract void write(int b) throws IOException;
}
