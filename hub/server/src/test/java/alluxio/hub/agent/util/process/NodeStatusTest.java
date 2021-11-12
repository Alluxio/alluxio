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

package alluxio.hub.agent.util.process;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;

import alluxio.exception.AlluxioException;
import alluxio.hub.proto.AlluxioNodeType;

import alluxio.hub.proto.AlluxioProcessStatusOrBuilder;
import alluxio.hub.proto.ProcessState;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

public class NodeStatusTest {

  @Test
  public void testNodeStatusMapping() {
    Arrays.stream(AlluxioNodeType.values())
        .filter(x -> x != AlluxioNodeType.ALL)
        .forEach(v -> assertNotNull(NodeStatus.lookupClass(v)));
  }

  @Test
  public void testGetNodeStatusSinglePid() throws IOException, AlluxioException {
    ProcessTable ps = Mockito.mock(ProcessTable.class);
    doReturn(Stream.of(1234)).when(ps).getJavaPids();
    doReturn(Collections.singletonList(1234)).when(ps)
        .getJavaPid(ArgumentMatchers.any(), ArgumentMatchers.any());
    NodeStatus s = new NodeStatus(ps);
    AlluxioProcessStatusOrBuilder stat = s.getProcessStatus(AlluxioNodeType.MASTER);
    assertEquals(1234, stat.getPid());
    assertEquals(AlluxioNodeType.MASTER, stat.getNodeType());
    assertEquals(ProcessState.RUNNING,  stat.getState());
  }

  @Test
  public void testGetNodeIllegalType() throws IOException, AlluxioException {
    NodeStatus s = new NodeStatus();
    assertThrows(IllegalArgumentException.class,
        () -> s.getProcessStatus(AlluxioNodeType.forNumber(1000)));
  }

  @Test
  public void testGetNodeStatusMultiplePid() throws IOException, AlluxioException {
    ProcessTable ps = Mockito.mock(ProcessTable.class);
    doReturn(Stream.of(1234, 12345)).when(ps).getJavaPids();
    doReturn(Arrays.asList(1234, 12345)).when(ps)
        .getJavaPid(ArgumentMatchers.any(), ArgumentMatchers.any());
    NodeStatus s = new NodeStatus(ps);
    assertThrows(IOException.class, () -> s.getProcessStatus(AlluxioNodeType.MASTER));
  }
}
