/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Utilities to do ser/de String, and ByteBuffer
 */
public class Utils {
  public static void writeString(String str, DataOutputStream os) throws IOException {
    if (str == null) {
      os.writeInt(-1);
    } else {
      os.writeInt(str.length());
      os.writeChars(str);
    }
  }

  public static String readString(DataInputStream is) throws IOException {
    int len = is.readInt();

    if (len == -1) {
      return null;
    } else if (len == 0) {
      return "";
    }

    char[] chars = new char[len];
    for (int k = 0; k < len; k ++) {
      chars[k] = is.readChar();
    }
    return new String(chars);

  }

  public static void writeByteBuffer(ByteBuffer buf, DataOutputStream os) throws IOException {
    if (buf == null) {
      os.writeInt(-1);
      return;
    }
    int len = buf.limit() - buf.position();
    os.writeInt(len);
    os.write(buf.array(), buf.position(), len);
  }

  public static ByteBuffer readByteBuffer(DataInputStream is) throws IOException {
    int len = is.readInt();
    if (len == -1) {
      return null;
    }

    byte[] arr = new byte[len];
    for (int k = 0; k < len ; k ++) {
      arr[k] = is.readByte();
    }

    return ByteBuffer.wrap(arr);
  }
}
