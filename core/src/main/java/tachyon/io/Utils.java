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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.codec.binary.Base64;

/**
 * Utilities to do ser/de String, and ByteBuffer
 */
public class Utils {
  /**
   * Converts a list of byte buffers to a list of base64-encoded Strings.
   */
  public static List<String> byteBufferListToBase64(List<ByteBuffer> bufs) {
    if (bufs == null) {
      return null;
    }

    List<String> bytes = Lists.newArrayListWithCapacity(bufs.size());
    for (ByteBuffer buf : bufs) {
      bytes.add(byteBufferToBase64(buf));
    }
    return bytes;
  }

  /**
   * Converts a byte buffer to a base64-encoded String. Avoids copying the array if possible.
   */
  public static String byteBufferToBase64(ByteBuffer buf) {
    if (buf == null) {
      return null;
    }

    if (buf.hasArray() && buf.position() == 0 && buf.limit() == buf.capacity()) {
      return Base64.encodeBase64String(buf.array());
    } else {
      byte[] b = new byte[buf.remaining()];
      buf.get(b);
      return Base64.encodeBase64String(b);
    }
  }

  public static ByteBuffer readByteBuffer(DataInputStream is) throws IOException {
    int len = is.readInt();
    if (len == -1) {
      return null;
    }

    byte[] arr = new byte[len];
    for (int k = 0; k < len; k ++) {
      arr[k] = is.readByte();
    }

    return ByteBuffer.wrap(arr);
  }

  public static List<ByteBuffer> readByteBufferList(DataInputStream is) throws IOException {
    int size = is.readInt();
    if (size == -1) {
      return null;
    }

    List<ByteBuffer> ret = new ArrayList<ByteBuffer>(size);
    for (int k = 0; k < size; k ++) {
      ret.add(readByteBuffer(is));
    }
    return ret;
  }

  public static List<Integer> readIntegerList(DataInputStream is) throws IOException {
    int size = is.readInt();
    if (size == -1) {
      return null;
    }
    List<Integer> ret = new ArrayList<Integer>(size);
    for (int k = 0; k < size; k ++) {
      ret.add(is.readInt());
    }
    return ret;
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

  public static List<String> readStringList(DataInputStream is) throws IOException {
    int size = is.readInt();

    if (size == -1) {
      return null;
    }

    List<String> ret = new ArrayList<String>(size);
    for (int k = 0; k < size; k ++) {
      ret.add(readString(is));
    }
    return ret;
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

  public static void writeByteBufferList(List<ByteBuffer> list, DataOutputStream os)
      throws IOException {
    if (list == null) {
      os.writeInt(-1);
      return;
    }
    os.writeInt(list.size());
    for (int k = 0; k < list.size(); k ++) {
      writeByteBuffer(list.get(k), os);
    }
  }

  public static void writeIntegerList(List<Integer> list, DataOutputStream os) throws IOException {
    if (list == null) {
      os.writeInt(-1);
      return;
    }

    os.writeInt(list.size());
    for (int k = 0; k < list.size(); k ++) {
      os.writeInt(list.get(k));
    }
  }

  public static void writeString(String str, DataOutputStream os) throws IOException {
    if (str == null) {
      os.writeInt(-1);
    } else {
      os.writeInt(str.length());
      os.writeChars(str);
    }
  }

  public static void writeStringList(List<String> list, DataOutputStream os) throws IOException {
    if (list == null) {
      os.writeInt(-1);
      return;
    }
    os.writeInt(list.size());
    for (int k = 0; k < list.size(); k ++) {
      writeString(list.get(k), os);
    }
  }
}
