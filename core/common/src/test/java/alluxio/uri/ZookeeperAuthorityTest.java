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
import static org.junit.Assert.assertNotEquals;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ZookeeperAuthorityTest {
  @Test
  public void permutationsUnordered() {
    testPermutationsEqual(ImmutableList.of("host1:port1", "host2:port2"));
    testPermutationsEqual(ImmutableList.of("host1:port1", "host2:port2", "host3:port3"));
    testPermutationsEqual(ImmutableList.of("host1:port1", "longHostname:somePort",
        "localhost:19998"));

    // test different host
    testPermutationsDifferent(ImmutableList.of("host1:port1", "host2:port2"),
        ImmutableList.of("host1:port1", "host3:port2"));
    // test different port
    testPermutationsDifferent(ImmutableList.of("host1:port1", "host2:port2"),
        ImmutableList.of("host1:port1", "host2:port3"));
    // test different numbers of addresses
    testPermutationsDifferent(ImmutableList.of("host1:port1", "host2:port2"),
        ImmutableList.of("host1:port1"));
    testPermutationsDifferent(ImmutableList.of("host1:port1", "host2:port2", "host3:port3"),
        ImmutableList.of("host1:port1", "host2:port2", "host4:port4"));
    testPermutationsDifferent(
        ImmutableList.of("host1:port1", "longHostname:somePort", "localhost:19998"),
        ImmutableList.of("host1:port1", "longHostname:differentPort", "remotehost:19998"));
  }

  private void testPermutationsEqual(List<String> zookeepers) {
    Set<String> allPermutations = Collections2.permutations(zookeepers)
        .stream()
        .map((list) -> String.join(",", list))
        .collect(Collectors.toSet());
    // make sure all address strings are different so none are skipped when put in a set
    assertEquals(IntMath.factorial(zookeepers.size()), allPermutations.size());

    for (Set<String> pair : Sets.combinations(allPermutations, 2)) {
      String[] two = pair.toArray(new String[0]);
      assertEquals(2, two.length);
      ZookeeperAuthority a1 = new ZookeeperAuthority(two[0]);
      ZookeeperAuthority a2 = new ZookeeperAuthority(two[1]);
      assertEquals(a1, a2);
      assertEquals(a1.hashCode(), a2.hashCode());
      assertEquals(a1.compareTo(a2), 0);
    }
  }

  private void testPermutationsDifferent(List<String> zookeeper1, List<String> zookeeper2) {
    Set<String> allPermutations1 = Collections2.permutations(zookeeper1)
        .stream()
        .map((list) -> String.join(",", list))
        .collect(Collectors.toSet());

    Set<String> allPermutations2 = Collections2.permutations(zookeeper2)
        .stream()
        .map((list) -> String.join(",", list))
        .collect(Collectors.toSet());

    // make sure all address strings are different so none are skipped when put in a set
    assertEquals(IntMath.factorial(zookeeper1.size()), allPermutations1.size());
    assertEquals(IntMath.factorial(zookeeper2.size()), allPermutations2.size());

    for (String authority1 : allPermutations1)
    {
      for (String authority2 : allPermutations2)
      {
        ZookeeperAuthority a1 = new ZookeeperAuthority(authority1);
        ZookeeperAuthority a2 = new ZookeeperAuthority(authority2);
        assertNotEquals(a1, a2);
        assertNotEquals(a1.hashCode(), a2.hashCode());
        assertNotEquals(a1.compareTo(a2), 0);
      }
    }
  }

  @Test
  public void equalsSingleZookeeper() {
    ZookeeperAuthority a1 = new ZookeeperAuthority("host1:port1");
    ZookeeperAuthority a1Duplicate = new ZookeeperAuthority("host1:port1");
    assertEquals(a1, a1Duplicate);
    assertEquals(a1.hashCode(), a1Duplicate.hashCode());
    assertEquals(a1.compareTo(a1Duplicate), 0);
  }

  @Test
  public void equalsEmpty() {
    ZookeeperAuthority empty = new ZookeeperAuthority("");
    ZookeeperAuthority alsoEmpty = new ZookeeperAuthority("");
    assertEquals(empty, alsoEmpty);
    assertEquals(empty.hashCode(), alsoEmpty.hashCode());
    assertEquals(empty.compareTo(alsoEmpty), 0);

    ZookeeperAuthority empty2 = new ZookeeperAuthority(",");
    ZookeeperAuthority alsoEmpty2 = new ZookeeperAuthority(",");
    assertEquals(empty2, alsoEmpty2);
    assertEquals(empty2.hashCode(), alsoEmpty2.hashCode());
    assertEquals(empty2.compareTo(alsoEmpty2), 0);
  }

  @Test
  public void notEqualEmptyNonEmpty() {
    ZookeeperAuthority nonEmpty = new ZookeeperAuthority("host1:port1,host2:port2");
    ZookeeperAuthority empty = new ZookeeperAuthority("");
    ZookeeperAuthority alsoEmpty = new ZookeeperAuthority(",");

    assertNotEquals(nonEmpty, empty);
    assertNotEquals(nonEmpty.hashCode(), empty.hashCode());
    assertNotEquals(nonEmpty.compareTo(empty), 0);

    assertNotEquals(nonEmpty, alsoEmpty);
    assertNotEquals(nonEmpty.hashCode(), alsoEmpty.hashCode());
    assertNotEquals(nonEmpty.compareTo(alsoEmpty), 0);
  }

  @Test
  public void notEqualEmptySegments() {
    ZookeeperAuthority empty = new ZookeeperAuthority("");
    ZookeeperAuthority seg1 = new ZookeeperAuthority(",");
    ZookeeperAuthority seg2 = new ZookeeperAuthority(",,");
    assertNotEquals(empty, seg1);
    assertNotEquals(empty.compareTo(seg1), 0);
    assertNotEquals(seg1, seg2);
    assertNotEquals(seg1.compareTo(seg2), 0);
    assertNotEquals(seg2, empty);
    assertNotEquals(seg2.compareTo(empty), 0);
  }

  @Test
  public void notEqualsDuplicates() {
    ZookeeperAuthority normal = new ZookeeperAuthority("host1:port1");
    ZookeeperAuthority insane = new ZookeeperAuthority("host1:port1,host1:port1");
    assertNotEquals(normal, insane);
    assertNotEquals(normal.compareTo(insane), 0);
  }
}
