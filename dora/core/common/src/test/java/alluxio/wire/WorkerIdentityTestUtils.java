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

import org.apache.commons.lang3.RandomUtils;

import java.util.UUID;

public class WorkerIdentityTestUtils {
  /**
   * @return a random long-based worker identity
   */
  public static WorkerIdentity randomLegacyId() {
    long id = RandomUtils.nextLong();
    return WorkerIdentity.ParserV0.INSTANCE.fromLong(id);
  }

  /**
   * @param id id
   * @return the identity that is based on the specified numeric id
   */
  public static WorkerIdentity ofLegacyId(long id) {
    return WorkerIdentity.ParserV0.INSTANCE.fromLong(id);
  }

  /**
   * @return a worker identity with a random UUID
   */
  public static WorkerIdentity randomUuidBasedId() {
    UUID uuid = UUID.randomUUID();
    return WorkerIdentity.ParserV1.INSTANCE.fromUUID(uuid);
  }

  /**
   * Creates an identity from the raw parts.
   * <p>
   * This directly calls the constructor of WorkerIdentity,
   * without going through all the necessary validations. Use of this method is likely to produce
   * invalid worker identities.
   *
   * @param id      the raw id bytes
   * @param version the version
   * @return worker identity
   */
  public static WorkerIdentity fromRawParts(byte[] id, int version) {
    return new WorkerIdentity(id, version);
  }
}
