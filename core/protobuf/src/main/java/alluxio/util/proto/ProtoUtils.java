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

package alluxio.util.proto;

import com.google.protobuf.CodedInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Protobuf related utils.
 */
public final class ProtoUtils {
  private ProtoUtils() {} // prevent instantiation

  /**
   * A wrapper of {@link CodedInputStream#readRawVarint32(InputStream)}.
   *
   * @param firstByte first byte in the input stream
   * @param input input stream
   * @return an int value read from the input stream
   * @throws IOException if the proto message being parsed is invalid
   */
  public static int readRawVarint32(int firstByte, InputStream input) throws IOException {
    return CodedInputStream.readRawVarint32(firstByte, input);
  }
}
