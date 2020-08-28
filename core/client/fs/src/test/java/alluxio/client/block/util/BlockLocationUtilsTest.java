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

package alluxio.client.block.util;

import static alluxio.client.util.ClientTestUtils.worker;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import alluxio.ConfigurationTestUtils;
import alluxio.Constants;
import alluxio.client.block.BlockWorkerInfo;
import alluxio.collections.Pair;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import alluxio.util.network.NettyUtils;
import alluxio.wire.WorkerNetAddress;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Tests {@link BlockLocationUtils}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(NettyUtils.class)
public final class BlockLocationUtilsTest {

  @Test
  public void chooseLocalAccessibleDomainSocket() throws Exception {
    List<BlockWorkerInfo> workers = new ArrayList<>();
    workers.add(worker(Constants.GB, "node2", "rack2"));
    // create worker info with domain socket path
    BlockWorkerInfo workerWithDomainSocket = worker(Constants.GB, "node3", "rack3");
    String domainSocketPath = "/tmp/domain/uuid-node3";
    workerWithDomainSocket.getNetAddress().setDomainSocketPath(domainSocketPath);
    workers.add(workerWithDomainSocket);

    // mock NettyUtils
    PowerMockito.mockStatic(NettyUtils.class);
    when(NettyUtils.isDomainSocketAccessible(eq(workerWithDomainSocket.getNetAddress()),
        any(AlluxioConfiguration.class))).thenReturn(true);

    // choose worker with domain socket accessible ignoring rack
    InstancedConfiguration conf = ConfigurationTestUtils.defaults();
    conf.set(PropertyKey.WORKER_DATA_SERVER_DOMAIN_SOCKET_AS_UUID, true);
    List<WorkerNetAddress> addresses = workers.stream()
        .map(worker -> worker.getNetAddress())
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    Optional<Pair<WorkerNetAddress, Boolean>> chosen = BlockLocationUtils
        .nearest(TieredIdentityFactory.fromString("node=node1,rack=rack2", conf), addresses, conf);
    assertTrue(chosen.isPresent());
    assertTrue(chosen.get().getSecond());
    assertEquals(domainSocketPath, chosen.get().getFirst().getDomainSocketPath());
    assertEquals("node3", chosen.get().getFirst().getHost());
  }
}
