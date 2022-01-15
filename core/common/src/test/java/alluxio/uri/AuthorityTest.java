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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    assertTrue(Authority.fromString("ebj@logical") instanceof EmbeddedLogicalAuthority);

    assertTrue(Authority.fromString("zk@logical") instanceof ZookeeperLogicalAuthority);

    assertTrue(Authority.fromString("localhost") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("f3,321:sad") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("localhost:") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("127.0.0.1:19998,") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("localhost:19998:8080") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("localhost:asdsad") instanceof UnknownAuthority);

    assertTrue(Authority.fromString("zk@") instanceof UnknownAuthority);
    assertTrue(Authority.fromString("zk@;") instanceof UnknownAuthority);
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

    SingleMasterAuthority authoritySame =
        (SingleMasterAuthority) Authority.fromString("localhost:19998");
    assertTrue(authority.equals(authoritySame));
    assertEquals(authority.compareTo(authoritySame), 0);
    assertEquals(authority.hashCode(), authoritySame.hashCode());

    SingleMasterAuthority authorityDiff =
        (SingleMasterAuthority) Authority.fromString("localhost:19999");
    assertFalse(authority.equals(authorityDiff));
    assertNotEquals(authority.compareTo(authorityDiff), 0);
    assertNotEquals(authority.hashCode(), authorityDiff.hashCode());
  }

  @Test
  public void multiMasterAuthorityTest() {
    // the equal, hashcode and comparison test is done in MultiMasterAuthorityTest
    MultiMasterAuthority authority =
        (MultiMasterAuthority) Authority.fromString("host1:19998,host2:19998,host3:19998");
    assertEquals("host1:19998,host2:19998,host3:19998", authority.toString());
    assertEquals("host1:19998,host2:19998,host3:19998", authority.getMasterAddresses());

    authority = (MultiMasterAuthority) Authority
        .fromString("127.0.0.1:213,127.0.0.2:532423,127.0.0.3:3213");
    assertEquals("127.0.0.1:213,127.0.0.2:532423,127.0.0.3:3213", authority.toString());
    assertEquals("127.0.0.1:213,127.0.0.2:532423,127.0.0.3:3213", authority.getMasterAddresses());

    authority = (MultiMasterAuthority) Authority.fromString("host1:19998;host2:19998;host3:19998");
    assertEquals("host1:19998,host2:19998,host3:19998", authority.getMasterAddresses());

    authority = (MultiMasterAuthority) Authority.fromString("host1:19998+host2:19998+host3:19998");
    assertEquals("host1:19998,host2:19998,host3:19998", authority.getMasterAddresses());

    assertFalse(Authority.fromString("localhost:19998")
        instanceof MultiMasterAuthority);
    assertFalse(Authority.fromString("localhost:abc,127.0.0.1:dsa")
        instanceof MultiMasterAuthority);
    assertFalse(Authority.fromString(",,,") instanceof MultiMasterAuthority);
    assertFalse(Authority.fromString(";;;") instanceof MultiMasterAuthority);
    assertFalse(Authority.fromString("+++") instanceof MultiMasterAuthority);
  }

  @Test
  public void zookeeperAuthorityTest() {
    // the equal, hashcode and comparison test is done in ZookeeperAuthorityTest
    ZookeeperAuthority authority = (ZookeeperAuthority) Authority.fromString("zk@host:2181");
    assertEquals("zk@host:2181", authority.toString());
    assertEquals("host:2181", authority.getZookeeperAddress());

    authority = (ZookeeperAuthority) Authority
        .fromString("zk@127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181");
    assertEquals("zk@127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181", authority.toString());
    assertEquals("127.0.0.1:2181,127.0.0.2:2181,127.0.0.3:2181", authority.getZookeeperAddress());

    authority = (ZookeeperAuthority) Authority.fromString("zk@host1:2181;host2:2181;host3:2181");
    assertEquals("zk@host1:2181,host2:2181,host3:2181", authority.toString());
    assertEquals("host1:2181,host2:2181,host3:2181", authority.getZookeeperAddress());

    authority = (ZookeeperAuthority) Authority.fromString("zk@host1:2181+host2:2181+host3:2181");
    assertEquals("zk@host1:2181,host2:2181,host3:2181", authority.toString());
    assertEquals("host1:2181,host2:2181,host3:2181", authority.getZookeeperAddress());
  }

  @Test
  public void zookeeperLogicalAuthorityTest() {
    ZookeeperLogicalAuthority authority =
        (ZookeeperLogicalAuthority) Authority.fromString("zk@logical");
    assertEquals("zk@logical", authority.toString());
    assertEquals("logical", authority.getLogicalName());

    ZookeeperLogicalAuthority authoritySame =
        (ZookeeperLogicalAuthority) Authority.fromString("zk@logical");
    assertTrue(authority.equals(authoritySame));
    assertEquals(authority.compareTo(authoritySame), 0);
    assertEquals(authority.hashCode(), authoritySame.hashCode());

    ZookeeperLogicalAuthority authorityDiff =
        (ZookeeperLogicalAuthority) Authority.fromString("zk@logical1");
    assertFalse(authority.equals(authorityDiff));
    assertNotEquals(authority.compareTo(authorityDiff), 0);
    assertNotEquals(authority.hashCode(), authorityDiff.hashCode());
  }

  @Test
  public void embeddedLogicalAuthorityTest() {
    EmbeddedLogicalAuthority authority =
        (EmbeddedLogicalAuthority) Authority.fromString("ebj@logical");
    assertEquals("ebj@logical", authority.toString());
    assertEquals("logical", authority.getLogicalName());

    EmbeddedLogicalAuthority authoritySame =
        (EmbeddedLogicalAuthority) Authority.fromString("ebj@logical");
    assertTrue(authority.equals(authoritySame));
    assertEquals(authority.compareTo(authoritySame), 0);
    assertEquals(authority.hashCode(), authoritySame.hashCode());

    EmbeddedLogicalAuthority authorityDiff =
        (EmbeddedLogicalAuthority) Authority.fromString("ebj@logical1");
    assertFalse(authority.equals(authorityDiff));
    assertNotEquals(authority.compareTo(authorityDiff), 0);
    assertNotEquals(authority.hashCode(), authorityDiff.hashCode());
  }

  @Test
  public void noAuthorityTest() {
    assertTrue(Authority.fromString("").equals(Authority.fromString(null)));
    assertEquals(Authority.fromString("").compareTo(Authority.fromString(null)), 0);
    assertEquals(Authority.fromString("").hashCode(), Authority.fromString(null).hashCode());
  }

  @Test
  public void differentAuthorityEqualTest() {
    String authorityAddress = "host:1122";
    List<Authority> authorities = new ArrayList<>();
    authorities.add(Authority.fromString(""));
    authorities.add(new ZookeeperAuthority(authorityAddress));
    authorities.add(new MultiMasterAuthority(authorityAddress));
    authorities.add(new EmbeddedLogicalAuthority(authorityAddress));
    authorities.add(new ZookeeperLogicalAuthority(authorityAddress));
    authorities.add(new UnknownAuthority(authorityAddress));
    authorities.add(new SingleMasterAuthority("host", 1122));

    for (int i = 0; i < authorities.size(); i++)
    {
      for (int j = 0; j < authorities.size(); j++)
      {
        if (i == j)
        {
          continue;
        }
        Authority a1 = authorities.get(i);
        Authority a2 = authorities.get(j);

        assertFalse(a1.equals(a2));
      }
    }
  }

  @Test
  public void mixedDelimiters() {
    String normalized = "a:0,b:0,c:0";
    for (String test : Arrays.asList(
        "zk@a:0;b:0+c:0",
        "zk@a:0,b:0;c:0",
        "zk@a:0+b:0,c:0"
    )) {
      assertEquals(normalized,
          ((ZookeeperAuthority) Authority.fromString(test)).getZookeeperAddress());
    }
  }
}
