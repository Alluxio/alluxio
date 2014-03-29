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
package tachyon.hadoop;

import java.io.IOException;
import java.io.OutputStream;

// import org.apache.hadoop.fs.Syncable;

import tachyon.client.TachyonFile;
import tachyon.client.WriteType;

public class HdfsFileOutputStream extends OutputStream { // implements Syncable

  HdfsFileOutputStream(TachyonFile file, WriteType writeType) {
    // TODO Auto-generated constructor stub
  }

  // @Override
  // public void sync() throws IOException {
  // // TODO Auto-generated method stub
  //
  // }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void flush() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(byte[] b) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public void write(int b) throws IOException {
    // TODO Auto-generated method stub

  }
}
