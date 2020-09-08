/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.transport.serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Serializer utils for write/read to/from streams.
 */
public class SerializerUtils {
  /**
   * Writes a string to output stream.
   *
   * @param outputStream output stream to write to
   * @param str the string to write
   */
  public static void writeStringToStream(DataOutputStream outputStream, String str)
      throws IOException {
    outputStream.writeInt(str.length());
    outputStream.writeBytes(str);
  }

  /**
   * Reads a string from stream.
   *
   * @param input input to read from
   * @return the read string
   */
  public static String readStringFromStream(DataInputStream input) throws IOException {
    int length = input.readInt();
    return new String(readBytesFromStream(input, length), StandardCharsets.UTF_8);
  }

  /**
   * Reads bytes from stream.
   *
   * @param input the input stream
   * @return the read bytes
   */
  public static byte[] readBytesFromStream(DataInputStream input) throws IOException {
    int length = input.readInt();
    return readBytesFromStream(input, length);
  }

  /**
   * Reads bytes up to a certain length from stream.
   *
   * @param input stream to read from
   * @param len the length to read
   * @return the read bytes
   */
  public static byte[] readBytesFromStream(DataInputStream input, int len) throws IOException {
    if (len == 0) {
      return new byte[0];
    }
    byte[] array = new byte[len];
    int offset = 0;
    int read;
    int toRead = len;
    while (toRead != 0) {
      read = input.read(array, offset, toRead);
      if (read == -1) {
        throw new IOException(String
            .format("Excepted to read %s bytes but only read %s bytes",
                len, offset));
      }
      offset += read;
      toRead -= read;
    }
    return array;
  }
}
