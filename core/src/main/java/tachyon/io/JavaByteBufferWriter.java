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
package tachyon.io;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * The wrapper for to write ByteBuffer using ByteBuffer's default methods.
 */
public class JavaByteBufferWriter extends ByteBufferWriter {

  public JavaByteBufferWriter(ByteBuffer buf) throws IOException {
    super(buf);
  }

  @Override
  public ByteBuffer getByteBuffer() {
    ByteBuffer buf = mBuf.duplicate();
    buf.position(0);
    buf.limit(mBuf.position());
    buf.order(mBuf.order());
    return buf;
  }

  @Override
  public void put(Byte b) {
    mBuf.put(b);
  }

  @Override
  public void put(byte[] src, int offset, int length) {
    mBuf.put(src, offset, length);
  }

  @Override
  public void putChar(char value) {
    mBuf.putChar(value);
  }

  @Override
  public void putDouble(double value) {
    mBuf.putDouble(value);
  }

  @Override
  public void putFloat(float value) {
    mBuf.putFloat(value);
  }

  @Override
  public void putInt(int value) {
    mBuf.putInt(value);
  }

  @Override
  public void putLong(long value) {
    mBuf.putLong(value);
  }

  @Override
  public void putShort(short value) {
    mBuf.putShort(value);
  }
}
