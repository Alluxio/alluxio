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
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

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
   */
  public static int readRawVarint32(int firstByte, InputStream input) throws IOException {
    return CodedInputStream.readRawVarint32(firstByte, input);
  }

  /**
   * A wrapper of
   * {@link alluxio.proto.journal.Job.TaskInfo.Builder#setResult} to take byte[] as input.
   *
   * @param builder the builder to update
   * @param bytes results bytes to set
   * @return updated builder
   */
  public static alluxio.proto.journal.Job.TaskInfo.Builder setResult(
      alluxio.proto.journal.Job.TaskInfo.Builder builder, byte[] bytes) {
    return builder.setResult(com.google.protobuf.ByteString.copyFrom(bytes));
  }

  /**
   * A wrapper of
   * {@link alluxio.proto.journal.Job.StartJobEntry.Builder#setSerializedJobConfig}
   * to take byte[] as input.
   *
   * @param builder the builder to update
   * @param bytes results bytes to set
   * @return updated builder
   */
  public static alluxio.proto.journal.Job.StartJobEntry.Builder setSerializedJobConfig(
      alluxio.proto.journal.Job.StartJobEntry.Builder builder, byte[] bytes) {
    return builder.setSerializedJobConfig(com.google.protobuf.ByteString.copyFrom(bytes));
  }

  /**
   * Checks whether the exception is an {@link InvalidProtocolBufferException} thrown because of
   * a truncated message.
   *
   * @param e the exception
   * @return whether the exception is an {@link InvalidProtocolBufferException} thrown because of
   *         a truncated message.
   */
  public static boolean isTruncatedMessageException(IOException e) {
    if (!(e instanceof InvalidProtocolBufferException)) {
      return false;
    }
    String truncatedMessage;
    try {
      Method method = InvalidProtocolBufferException.class.getMethod("truncatedMessage");
      method.setAccessible(true);
      truncatedMessage = (String) method.invoke(null);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException ee) {
      throw new RuntimeException(ee);
    }
    return e.getMessage().equals(truncatedMessage);
  }
}
