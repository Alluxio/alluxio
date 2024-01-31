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

import static org.junit.Assert.assertNotEquals;

import alluxio.grpc.GrpcUtils;
import alluxio.util.CommonUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Defaults;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Random;

public class WorkerNetAddressTest {

  @Test
  public void json() throws Exception {
    WorkerNetAddress workerNetAddress = createRandom();
    ObjectMapper mapper = new ObjectMapper();
    WorkerNetAddress other =
        mapper.readValue(mapper.writeValueAsBytes(workerNetAddress), WorkerNetAddress.class);
    checkEquality(workerNetAddress, other);
  }

  @Test
  public void proto() {
    WorkerNetAddress workerNetAddress = createRandom();
    WorkerNetAddress other = GrpcUtils.fromProto(GrpcUtils.toProto(workerNetAddress));
    checkEquality(workerNetAddress, other);
  }

  @Test
  public void copyConstructor() throws IllegalAccessException {
    WorkerNetAddress original = new WorkerNetAddress()
        .setHost("host")
        .setContainerHost("container")
        .setRpcPort(1)
        .setDataPort(1)
        .setNettyDataPort(1)
        .setSecureRpcPort(1)
        .setWebPort(1)
        .setDomainSocketPath("path")
        .setHttpServerPort(1);
    WorkerNetAddress copied = new WorkerNetAddress(original);
    // copied instance should contain exactly the same content
    checkEquality(original, copied);
    // mutate any non-final field in the copy,
    // and the change should not be reflected in the original
    for (Field field : WorkerNetAddress.class.getDeclaredFields()) {
      int fieldModifiers = field.getModifiers();
      if (Modifier.isStatic(fieldModifiers) || Modifier.isFinal(fieldModifiers)) {
        continue;
      }
      field.setAccessible(true);
      // set fields in the copy to their default value
      field.set(copied, Defaults.defaultValue(field.getType()));
      assertNotEquals(field.getName(), field.get(original), field.get(copied));
    }
  }

  public void checkEquality(WorkerNetAddress a, WorkerNetAddress b) {
    Assert.assertEquals(a.getHost(), b.getHost());
    Assert.assertEquals(a.getRpcPort(), b.getRpcPort());
    Assert.assertEquals(a.getDataPort(), b.getDataPort());
    Assert.assertEquals(a.getWebPort(), b.getWebPort());
    Assert.assertEquals(a, b);
  }

  public static WorkerNetAddress createRandom() {
    WorkerNetAddress result = new WorkerNetAddress();
    Random random = new Random();

    String host = CommonUtils.randomAlphaNumString(random.nextInt(10));
    int rpcPort = random.nextInt();
    int dataPort = random.nextInt();
    int webPort = random.nextInt();

    result.setHost(host);
    result.setRpcPort(rpcPort);
    result.setDataPort(dataPort);
    result.setWebPort(webPort);

    return result;
  }
}
