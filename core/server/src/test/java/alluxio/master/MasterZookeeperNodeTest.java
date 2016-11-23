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

package alluxio.master;

import static org.junit.Assert.assertEquals;

import com.google.common.testing.EqualsTester;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link MasterZookeeperNode}.
 */
public final class MasterZookeeperNodeTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void equals() {
    new EqualsTester()
        .addEqualityGroup(new MasterZookeeperNode("a", 1, 2), new MasterZookeeperNode("a", 1, 2))
        .addEqualityGroup(new MasterZookeeperNode("b", 1, 2))
        .addEqualityGroup(new MasterZookeeperNode("a", 2, 2))
        .addEqualityGroup(new MasterZookeeperNode("a", 1, 1))
        .addEqualityGroup(new MasterZookeeperNode("a", 2, 1))
        .testEquals();
  }

  @Test
  public void sanity() {
    testSerde("localhost", 1, 2);
  }

  @Test
  public void specialChars() {
    testSerde("abc?/\\$!_~`-", 0, 0);
  }

  @Test
  public void rpcPortNotInteger() {
    mThrown.expect(RuntimeException.class);
    MasterZookeeperNode.deserialize("a:b:0");
  }

  @Test
  public void webPortNotInteger() {
    mThrown.expect(RuntimeException.class);
    MasterZookeeperNode.deserialize("a:0:b");
  }

  @Test
  public void notEnoughParts() {
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage("Master zookeeper nodes must be in the form name:rpcPort:webPort, but " +
        "the specified node has name 'a:0'");
    MasterZookeeperNode.deserialize("a:0");
  }

  private void testSerde(String hostname, int rpcPort, int webPort) {
    MasterZookeeperNode node1 = new MasterZookeeperNode(hostname, rpcPort, webPort);
    MasterZookeeperNode node2 = MasterZookeeperNode.deserialize(node1.serialize());
    assertEquals(node1, node2);
  }
}
