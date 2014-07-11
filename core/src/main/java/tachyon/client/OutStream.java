/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.client;

import java.io.IOException;
import java.io.OutputStream;

import tachyon.conf.UserConf;

/**
 * <code>OutStream</code> is the base output stream class for TachyonFile streaming output methods.
 * It can only be gotten by calling the methods in <code>tachyon.client.TachyonFile</code>, but
 * can not be initialized by the client code.
 */
public abstract class OutStream extends OutputStream {
  protected final UserConf USER_CONF = UserConf.get();
  protected final TachyonFile FILE;
  protected final TachyonFS TFS;
  protected final WriteType WRITE_TYPE;

  /**
   * @param file
   *          the output file of the OutStream
   * @param writeType
   *          the OutStream's write type
   */
  OutStream(TachyonFile file, WriteType writeType) {
    FILE = file;
    TFS = FILE.TFS;
    WRITE_TYPE = writeType;
  }

  /**
   * Cancel the write operations to the OutStream
   * 
   * @throws IOException
   */
  public abstract void cancel() throws IOException;

  @Override
  public abstract void close() throws IOException;

  @Override
  public abstract void flush() throws IOException;

  @Override
  public abstract void write(byte b[]) throws IOException;

  @Override
  public abstract void write(byte b[], int off, int len) throws IOException;

  @Override
  public abstract void write(int b) throws IOException;
}