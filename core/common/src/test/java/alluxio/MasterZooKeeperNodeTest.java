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

package alluxio;

import static org.junit.Assert.assertEquals;

import com.google.common.testing.EqualsTester;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for {@link MasterZooKeeperNode}.
 */
public final class MasterZooKeeperNodeTest {

  @Rule
  public ExpectedException mThrown = ExpectedException.none();

  @Test
  public void equals() {
    new EqualsTester()
        .addEqualityGroup(new MasterZooKeeperNode("a", 1, 2), new MasterZooKeeperNode("a", 1, 2))
        .addEqualityGroup(new MasterZooKeeperNode("b", 1, 2))
        .addEqualityGroup(new MasterZooKeeperNode("a", 2, 2))
        .addEqualityGroup(new MasterZooKeeperNode("a", 1, 1))
        .addEqualityGroup(new MasterZooKeeperNode("a", 2, 1))
        .testEquals();
  }

  @Test
  public void serialize() {
    MasterZooKeeperNode node = new MasterZooKeeperNode("host", 19998, 19999);
    assertEquals("host:19998:19999", node.serialize());
  }

  @Test
  public void deserialize() {
    MasterZooKeeperNode node = MasterZooKeeperNode.deserialize("host:19998:19999");
    assertEquals("host", node.getHostname());
    assertEquals(19998, node.getRpcPort());
    assertEquals(19999, node.getWebPort());
  }

  @Test
  public void sanity() {
    testSerializationDeserialization("localhost", 1, 2);
  }

  @Test
  public void specialChars() {
    testSerializationDeserialization("abc?/\\$!_~`-.", 0, 0);
  }

  @Test
  public void getHostname() {
    MasterZooKeeperNode node = new MasterZooKeeperNode("testhost", 1, 2);
    assertEquals("testhost", node.getHostname());
  }

  @Test
  public void getRpcPort() {
    MasterZooKeeperNode node = new MasterZooKeeperNode("testhost", 1, 2);
    assertEquals(1, node.getRpcPort());
  }

  @Test
  public void getWebPort() {
    MasterZooKeeperNode node = new MasterZooKeeperNode("testhost", 1, 2);
    assertEquals(2, node.getWebPort());
  }

  @Test
  public void rpcPortNotInteger() {
    mThrown.expect(RuntimeException.class);
    MasterZooKeeperNode.deserialize("a:b:0");
  }

  @Test
  public void webPortNotInteger() {
    mThrown.expect(RuntimeException.class);
    MasterZooKeeperNode.deserialize("a:0:b");
  }

  @Test
  public void notEnoughParts() {
    mThrown.expect(RuntimeException.class);
    mThrown.expectMessage("Master zookeeper nodes must be in the form name:rpcPort:webPort, but "
        + "the specified node has name 'a:0'");
    MasterZooKeeperNode.deserialize("a:0");
  }

  private void testSerializationDeserialization(String hostname, int rpcPort, int webPort) {
    MasterZooKeeperNode node1 = new MasterZooKeeperNode(hostname, rpcPort, webPort);
    MasterZooKeeperNode node2 = MasterZooKeeperNode.deserialize(node1.serialize());
    assertEquals(node1, node2);
  }
}
