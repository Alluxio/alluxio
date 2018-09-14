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

package alluxio.uri;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit tests for {@link Authority}.
 */
public class AuthorityTest {

  @Test
  public void authorityFromStringTest() {
    assertTrue(Authority.fromString("localhost:19998") instanceof SingleMasterAuthority);
    assertTrue(Authority.fromString("127.0.0.1:19998") instanceof SingleMasterAuthority);

    assertTrue(Authority.fromString("zk@host:2181") instanceof ZookeeperAuthority);
    assertTrue(Authority.fromString("zk@host1:2181,127.0.0.2:2181,12.43.214.53:2181")
        instanceof ZookeeperAuthority);
    assertTrue(Authority.fromString("zk@host1:2181;host2:2181;host3:2181")
        instanceof ZookeeperAuthority);

    assertTrue(Authority.fromString("") instanceof NoAuthority);
    assertTrue(Authority.fromString(null) instanceof NoAuthority);

    assertTrue(Authority.fromString("localhost") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("f3,321:sad") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("localhost:") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("127.0.0.1:19998,") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("localhost:19998:8080") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("localhost:asdsad") instanceof UnknownAuthority);

    assertTrue(Authority.fromString("zk@") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("zk@;") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("zk@localhost") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("zk@127.0.0.1:port") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("zk@127.0.0.1:2181,") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("zk@127.0.0.1:2181,localhost") instanceof UnknownAuthority);

    assertTrue(Authority.fromString(",,,") instanceof UnknownAuthority);
    assertTrue(Authority.fromString(";;;") instanceof UnknownAuthority);
  }

  @Test
  public void singleMasterAuthorityTest() {
    SingleMasterAuthority authority =
        (SingleMasterAuthority) Authority.fromString("localhost:19998");
    assertEquals("localhost:19998", authority.toString());
    assertEquals("localhost", authority.getHost());
    assertEquals(19998, authority.getPort());
  }

  @Test
  public void zookeeperAuthorityTest() {
    ZookeeperAuthority authority = (ZookeeperAuthority) Authority.fromString("zk@host:2181");
    assertEquals("zk@host:2181", authority.toString());
    assertEquals("host:2181", authority.getZookeeperAddress());

    authority = (ZookeeperAuthority) Authority
        .fromString("zk@127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181");
    assertEquals("zk@127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181", authority.toString());
    assertEquals("127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181", authority.getZookeeperAddress());

    authority = (ZookeeperAuthority) Authority.fromString("zk@host1:2181;host2:2181;host3:2181");
    assertEquals("zk@host1:2181;host2:2181;host3:2181", authority.toString());
    assertEquals("host1:2181,host2:2181,host3:2181", authority.getZookeeperAddress());

    authority = (ZookeeperAuthority) Authority.fromString("zk@host1:2181+host2:2181+host3:2181");
    assertEquals("zk@host1:2181+host2:2181+host3:2181", authority.toString());
    assertEquals("host1:2181,host2:2181,host3:2181", authority.getZookeeperAddress());
  }
}
