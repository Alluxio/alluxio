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

package alluxio.wire;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for conversion between wire types and grpc types.
 */
@ThreadSafe
public final class ProtoUtils {

  private ProtoUtils() {} // prevent instantiation

  /**
   * Converts a proto type to a wire type.
   *
   * @param fileInfo the proto representation of a file information
   * @return wire representation of the file information
   */
  public static FileInfo fromProto(alluxio.grpc.FileInfo fileInfo) {
    return new FileInfo(fileInfo);
  }

  /**
   * Converts thrift type to wire type.
   *
   * @param tTtlAction {@link TTtlAction}
   * @return {@link TtlAction} equivalent
   */
  public static TtlAction fromProto(alluxio.grpc.TtlAction tTtlAction) {
    return TtlAction.fromProto(tTtlAction);
  }

  /**
   * Converts a wire type to a proto type.
   *
   * @param fileInfo the wire representation of a file information
   * @return proto representation of the file information
   */
  public static alluxio.grpc.FileInfo toProto(FileInfo fileInfo) {
    return fileInfo.toProto();
  }

  /**
   * Converts wire type to thrift type.
   *
   * @param ttlAction {@link TtlAction}
   * @return {@link TTtlAction} equivalent
   */
  public static alluxio.grpc.TtlAction toProto(TtlAction ttlAction) {
    return TtlAction.toProto(ttlAction);
  }
}

