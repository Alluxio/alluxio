package alluxio.membership;

import alluxio.wire.WorkerNetAddress;

import org.junit.Test;

public final class ServiceEntityTest {

  @Test
  public void testSerializationWorkerServiceEntity() {
    WorkerServiceEntity entity = new WorkerServiceEntity(new WorkerNetAddress()
        .setHost("worker1").setContainerHost("containerhostname1")
        .setRpcPort(1000).setDataPort(1001).setWebPort(1011)
        .setDomainSocketPath("/var/lib/domain.sock"));
    String str = ServiceEntity.toJson(entity);
    ServiceEntity deserialized = WorkerServiceEntity.fromJson(str);
    assert(deserialized.equals(entity));
  }
}
