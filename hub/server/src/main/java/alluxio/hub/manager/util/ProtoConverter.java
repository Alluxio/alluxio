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

package alluxio.hub.manager.util;

import com.google.protobuf.GeneratedMessageV3;

/**
 * Interface to govern that an object must be able to convert itself to a proto representation.
 *
 * The reason that we use intermediate objects instead of the proto directly is to provide thread
 * -safety which the generated proto objects do not support.
 *
 * @param <T> the proto type that this object can be converted to
 */
public interface ProtoConverter<T extends GeneratedMessageV3> {

  /**
   * Converts an objects representation into a protobuf type.
   *
   * @return the object as a proto representation
   */
  T toProto();
}
