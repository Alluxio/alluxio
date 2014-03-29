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
 * The wrapper for to read ByteBuffer using ByteBuffer's default methods.
 */
public class JavaByteBufferReader extends ByteBufferReader {
  public JavaByteBufferReader(ByteBuffer buf) throws IOException {
    super(buf);
  }

  @Override
  public byte get() {
    return mBuf.get();
  }

  @Override
  public void get(byte[] dst) {
    mBuf.get(dst);
  }

  @Override
  public void get(byte[] dst, int offset, int length) {
    mBuf.get(dst, offset, length);
  }

  @Override
  public char getChar() {
    return mBuf.getChar();
  }

  @Override
  public double getDouble() {
    return mBuf.getDouble();
  }

  @Override
  public float getFloat() {
    return mBuf.getFloat();
  }

  @Override
  public int getInt() {
    return mBuf.getInt();
  }

  @Override
  public long getLong() {
    return mBuf.getLong();
  }

  @Override
  public short getShort() {
    return mBuf.getShort();
  }

  @Override
  public int position() {
    return mBuf.position();
  }

  @Override
  public void position(int newPosition) {
    mBuf.position(newPosition);
  }
}
