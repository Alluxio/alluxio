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

  @Test
  public void testEqualsIgnoringOptionalFields() throws Exception {
    final WorkerNetAddress workerNetAddress1 = new WorkerNetAddress()
        .setHost("worker1").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(1021);
    final WorkerNetAddress workerNetAddress2 = new WorkerNetAddress()
        .setHost("worker1").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(2021);
    final WorkerNetAddress workerNetAddress3 = new WorkerNetAddress()
        .setHost("worker3").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock").setHttpServerPort(1021);
    final WorkerIdentity identity = WorkerIdentity.fromProto(
        alluxio.grpc.WorkerIdentity.newBuilder()
            .setIdentifier(ByteString.copyFrom(Longs.toByteArray(1L)))
            .setVersion(0)
            .build());
    WorkerServiceEntity entity1 = new WorkerServiceEntity(identity, workerNetAddress1);
    WorkerServiceEntity entity2 = new WorkerServiceEntity(identity, workerNetAddress2);
    WorkerServiceEntity entity3 = new WorkerServiceEntity(identity, workerNetAddress3);

    Assert.assertTrue(entity1.equalsIgnoringOptionalFields(entity2));
    Assert.assertFalse(entity2.equalsIgnoringOptionalFields(entity3));
    Assert.assertFalse(entity3.equalsIgnoringOptionalFields(entity1));
  }
}
