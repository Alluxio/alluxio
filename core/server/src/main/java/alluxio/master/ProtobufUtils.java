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

import alluxio.proto.journal.File.PTtlAction;
import alluxio.wire.TtlAction;

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

    TtlAction ttlAction = TtlAction.DELETE;
    if (pTtlAction != null) {
      switch (pTtlAction) {
        case DELETE:
          ttlAction = TtlAction.DELETE;
          break;
        case FREE:
          ttlAction = TtlAction.FREE;
          break;
        default:
          ttlAction = TtlAction.DELETE;
          break;
      }
    }
    return ttlAction;
  }

  /**
   * Converts Wire type to Protobuf type.
   *
   * @param ttlAction {@link PTtlAction}
   * @return {@link TtlAction} equivalent
   */
  public static PTtlAction toProtobuf(TtlAction ttlAction) {

    PTtlAction pTtlAction = PTtlAction.DELETE;
    if (ttlAction != null) {
      switch (ttlAction) {
        case DELETE:
          pTtlAction = PTtlAction.DELETE;
          break;
        case FREE:
          pTtlAction = PTtlAction.FREE;
          break;
        default:
          pTtlAction = PTtlAction.DELETE;
          break;
      }
    }
    return pTtlAction;
  }
}
