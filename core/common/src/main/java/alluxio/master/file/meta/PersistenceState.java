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

package alluxio.master.file.meta;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The persistence state of a file in the under-storage system.
 */
@ThreadSafe
public enum PersistenceState {
  NOT_PERSISTED, // file not persisted in the under FS
  TO_BE_PERSISTED, // the file is to be persisted in the under FS
  PERSISTED, // the file is persisted in the under FS
  LOST; // the file is lost but not persisted in the under FS

  /**
   * @return the persistence state as a protobuf object
   */
  public alluxio.grpc.PersistenceState toProto() {
    switch (this) {
      case NOT_PERSISTED:
        return alluxio.grpc.PersistenceState.NOT_PERSISTED;
      case TO_BE_PERSISTED:
        return alluxio.grpc.PersistenceState.TO_BE_PERSISTED;
      case PERSISTED:
        return alluxio.grpc.PersistenceState.PERSISTED;
      case LOST:
        return alluxio.grpc.PersistenceState.LOST;
      default:
        throw new IllegalStateException(String.format("Invalid persistence state %s", this));
    }
  }

  /**
   * @param state the protobuf persistence state
   * @return the persistence state
   */
  public static PersistenceState fromProto(alluxio.grpc.PersistenceState state) {
    switch (state) {
      case NOT_PERSISTED:
        return NOT_PERSISTED;
      case TO_BE_PERSISTED:
        return TO_BE_PERSISTED;
      case PERSISTED:
        return PERSISTED;
      case LOST:
        return LOST;
      default:
        throw new IllegalStateException(String.format("Invalid persistence state %s", state));
    }
  }
}
