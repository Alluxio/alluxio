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

package alluxio.membership;

import alluxio.wire.WorkerIdentity;
import alluxio.wire.WorkerNetAddress;

import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;

public final class ServiceEntityTest {

  @Test
  public void testSerializationWorkerServiceEntity() throws Exception {
    final WorkerNetAddress workerNetAddress = new WorkerNetAddress()
        .setHost("worker1").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock");
    final WorkerIdentity identity = WorkerIdentity.fromProto(
        alluxio.grpc.WorkerIdentity.newBuilder()
            .setIdentifier(ByteString.copyFrom(Longs.toByteArray(1L)))
            .setVersion(0)
            .build());
    WorkerServiceEntity entity = new WorkerServiceEntity(identity, workerNetAddress);
    byte[] jsonBytes = entity.serialize();
    DefaultServiceEntity deserialized = new WorkerServiceEntity();
    deserialized.deserialize(jsonBytes);
    Assert.assertEquals(deserialized, entity);
  }
}
