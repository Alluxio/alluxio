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

package alluxio.master;

import alluxio.grpc.TtlAction;
import alluxio.proto.journal.File.PTtlAction;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Utility methods for conversion between wire types and protobuf types.
 */
@ThreadSafe
public final class ProtobufUtils {

  private ProtobufUtils() {} // prevent instantiation

  /**
   * Converts Protobuf type to Wire type.
   *
   * @param pTtlAction {@link PTtlAction}
   * @return {@link TtlAction} equivalent
   */
  public static TtlAction fromProtobuf(PTtlAction pTtlAction) {
    if (pTtlAction == null) {
      return TtlAction.DELETE;
    }
    switch (pTtlAction) {
      case DELETE:
        return TtlAction.DELETE;
      case FREE:
        return TtlAction.FREE;
      default:
        throw new IllegalStateException("Unknown protobuf ttl action: " + pTtlAction);
    }
  }

  /**
   * Converts Wire type to Protobuf type.
   *
   * @param ttlAction {@link PTtlAction}
   * @return {@link TtlAction} equivalent
   */
  public static PTtlAction toProtobuf(TtlAction ttlAction) {
    if (ttlAction == null) {
      return PTtlAction.DELETE;
    }
    switch (ttlAction) {
      case DELETE:
        return PTtlAction.DELETE;
      case FREE:
        return PTtlAction.FREE;
      default:
        throw new IllegalStateException("Unknown ttl action: " + ttlAction);
    }
  }
}
