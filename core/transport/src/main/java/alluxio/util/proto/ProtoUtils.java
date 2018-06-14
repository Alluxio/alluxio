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

import alluxio.exception.status.Status;
import alluxio.proto.status.Status.PStatus;

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
    /**
   * Converts an internal exception status to a protocol buffer type status.
   *
   * @param status the status to convert
   * @return the protocol buffer type status
   */
  public static PStatus toProto(Status status) {
    switch (status) {
      case ABORTED:
        return PStatus.ABORTED;
      case ALREADY_EXISTS:
        return PStatus.ALREADY_EXISTS;
      case CANCELED:
        return PStatus.CANCELED;
      case DATA_LOSS:
        return PStatus.DATA_LOSS;
      case DEADLINE_EXCEEDED:
        return PStatus.DEADLINE_EXCEEDED;
      case FAILED_PRECONDITION:
        return PStatus.FAILED_PRECONDITION;
      case INTERNAL:
        return PStatus.INTERNAL;
      case INVALID_ARGUMENT:
        return PStatus.INVALID_ARGUMENT;
      case NOT_FOUND:
        return PStatus.NOT_FOUND;
      case OK:
        return PStatus.OK;
      case OUT_OF_RANGE:
        return PStatus.OUT_OF_RANGE;
      case PERMISSION_DENIED:
        return PStatus.PERMISSION_DENIED;
      case RESOURCE_EXHAUSTED:
        return PStatus.RESOURCE_EXHAUSTED;
      case UNAUTHENTICATED:
        return PStatus.UNAUTHENTICATED;
      case UNAVAILABLE:
        return PStatus.UNAVAILABLE;
      case UNIMPLEMENTED:
        return PStatus.UNIMPLEMENTED;
      case UNKNOWN:
        return PStatus.UNKNOWN;
      default:
        return PStatus.UNKNOWN;
    }
  }

  /**
   * Creates a {@link Status} from a protocol buffer type status.
   *
   * @param status the protocol buffer type status
   * @return the corresponding {@link Status}
   */
  public static Status fromProto(alluxio.proto.status.Status.PStatus status) {
    switch (status) {
      case ABORTED:
        return Status.ABORTED;
      case ALREADY_EXISTS:
        return Status.ALREADY_EXISTS;
      case CANCELED:
        return Status.CANCELED;
      case DATA_LOSS:
        return Status.DATA_LOSS;
      case DEADLINE_EXCEEDED:
        return Status.DEADLINE_EXCEEDED;
      case FAILED_PRECONDITION:
        return Status.FAILED_PRECONDITION;
      case INTERNAL:
        return Status.INTERNAL;
      case INVALID_ARGUMENT:
        return Status.INVALID_ARGUMENT;
      case NOT_FOUND:
        return Status.NOT_FOUND;
      case OK:
        return Status.OK;
      case OUT_OF_RANGE:
        return Status.OUT_OF_RANGE;
      case PERMISSION_DENIED:
        return Status.PERMISSION_DENIED;
      case RESOURCE_EXHAUSTED:
        return Status.RESOURCE_EXHAUSTED;
      case UNAUTHENTICATED:
        return Status.UNAUTHENTICATED;
      case UNAVAILABLE:
        return Status.UNAVAILABLE;
      case UNIMPLEMENTED:
        return Status.UNIMPLEMENTED;
      case UNKNOWN:
        return Status.UNKNOWN;
      default:
        return Status.UNKNOWN;
    }
  }
}
