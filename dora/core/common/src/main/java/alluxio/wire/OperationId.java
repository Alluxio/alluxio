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

import alluxio.collections.Pair;
import alluxio.grpc.FsOpPId;
import alluxio.proto.journal.Journal;

import java.util.UUID;

/**
 * Used as an unique ID wrapper.
 */
public class OperationId extends Pair<Long, Long> {

  /**
   * Constructs and initializes a 128 bit OperationId.
   *
   * @param leastSignificant the least significant bits
   * @param mostSignificant the most significant bits
   */
  public OperationId(Long leastSignificant, Long mostSignificant) {
    super(leastSignificant, mostSignificant);
  }

  /**
   * Constructs and initializes a 128 bit OperationId from an UUID.
   *
   * @param guid the guid
   */
  public OperationId(UUID guid) {
    super(guid.getLeastSignificantBits(), guid.getMostSignificantBits());
  }

  /**
   * @return proto representation of fs operation id
   */
  public FsOpPId toFsProto() {
    return FsOpPId.newBuilder().setLeastSignificantBits(getFirst())
        .setMostSignificantBits(getSecond()).build();
  }

  /**
   * @return proto representation of fs operation id
   */
  public Journal.JournalOpPId toJournalProto() {
    return Journal.JournalOpPId.newBuilder().setLeastSignificantBits(getFirst())
        .setMostSignificantBits(getSecond()).build();
  }

  /**
   * Creates FsOpId from proto.
   *
   * @param opId proto op id
   * @return wire fs op id
   */
  public static OperationId fromFsProto(FsOpPId opId) {
    return new OperationId(opId.getLeastSignificantBits(), opId.getMostSignificantBits());
  }

  /**
   * Creates FsOpId from proto.
   *
   * @param opId proto op id
   * @return wire fs op id
   */
  public static OperationId fromJournalProto(Journal.JournalOpPId opId) {
    return new OperationId(opId.getLeastSignificantBits(), opId.getMostSignificantBits());
  }
}
