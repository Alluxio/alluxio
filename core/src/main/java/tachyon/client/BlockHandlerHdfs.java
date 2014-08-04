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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * it is used for handling block files on HDFS.
 */
// TODO how to simulate read & write with HDFS just like localFS (using ByteBuffer)
public class BlockHandlerHdfs extends BlockHandler {

  public BlockHandlerHdfs(String path, Object conf) throws IOException {
    super(path);

  }

  @Override
  public int appendCurrentBuffer(byte[] buf, long inFileBytes, int offset, int length)
      throws IOException, FileNotFoundException {

    return 0;
  }

  @Override
  public void close() throws IOException {

    return;
  }

  @Override
  public void delete() {

    return;
  }

  @Override
  public ByteBuffer readByteBuffer(int offset, int length) throws IOException,
      FileNotFoundException {

    return ByteBuffer.allocate(0);
  }
}
