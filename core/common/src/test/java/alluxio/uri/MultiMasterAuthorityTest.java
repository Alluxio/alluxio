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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.math.IntMath;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class MultiMasterAuthorityTest {
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
    // test different numbers of masters
    testPermutationsDifferent(ImmutableList.of("host1:port1", "host2:port2"),
        ImmutableList.of("host1:port1"));
    testPermutationsDifferent(ImmutableList.of("host1:port1", "host2:port2", "host3:port3"),
        ImmutableList.of("host1:port1", "host2:port2", "host4:port4"));
    testPermutationsDifferent(
        ImmutableList.of("host1:port1", "longHostname:somePort", "localhost:19998"),
        ImmutableList.of("host1:port1", "longHostname:differentPort", "remotehost:19998"));
  }

  private void testPermutationsEqual(List<String> masters) {
    Set<String> allPermutations = Collections2.permutations(masters)
        .stream()
        .map((list) -> String.join(",", list))
        .collect(Collectors.toSet());
    // make sure all master strings are different so none are skipped when put in a set
    assertEquals(IntMath.factorial(masters.size()), allPermutations.size());

    for (Set<String> pair : Sets.combinations(allPermutations, 2)) {
      String[] two = pair.toArray(new String[0]);
      assertEquals(2, two.length);
      MultiMasterAuthority a1 = new MultiMasterAuthority(two[0]);
      MultiMasterAuthority a2 = new MultiMasterAuthority(two[1]);
      assertTrue(a1.equals(a2));
      assertEquals(a1.hashCode(), a2.hashCode());
      assertEquals(a1.compareTo(a2), 0);
    }
  }

  private void testPermutationsDifferent(List<String> master1, List<String> master2) {
    Set<String> allPermutations1 = Collections2.permutations(master1)
        .stream()
        .map((list) -> String.join(",", list))
        .collect(Collectors.toSet());

    Set<String> allPermutations2 = Collections2.permutations(master2)
        .stream()
        .map((list) -> String.join(",", list))
        .collect(Collectors.toSet());

    // make sure all master strings are different so none are skipped when put in a set
    assertEquals(IntMath.factorial(master1.size()), allPermutations1.size());
    assertEquals(IntMath.factorial(master2.size()), allPermutations2.size());

    for (String authority1 : allPermutations1)
    {
      for (String authority2 : allPermutations2)
      {
        MultiMasterAuthority a1 = new MultiMasterAuthority(authority1);
        MultiMasterAuthority a2 = new MultiMasterAuthority(authority2);
        assertFalse(a1.equals(a2));
        assertNotEquals(a1.hashCode(), a2.hashCode());
        assertNotEquals(a1.compareTo(a2), 0);
      }
    }
  }

  @Test
  public void equalsSingleMaster() {
    MultiMasterAuthority a1 = new MultiMasterAuthority("host1:port1");
    MultiMasterAuthority a1Duplicate = new MultiMasterAuthority("host1:port1");
    assertTrue(a1.equals(a1Duplicate));
    assertEquals(a1.hashCode(), a1Duplicate.hashCode());
    assertEquals(a1.compareTo(a1Duplicate), 0);
  }

  @Test
  public void equalsEmpty() {
    MultiMasterAuthority empty = new MultiMasterAuthority("");
    MultiMasterAuthority alsoEmpty = new MultiMasterAuthority("");
    assertTrue(empty.equals(alsoEmpty));
    assertEquals(empty.hashCode(), alsoEmpty.hashCode());
    assertEquals(empty.compareTo(alsoEmpty), 0);

    MultiMasterAuthority empty2 = new MultiMasterAuthority(",");
    MultiMasterAuthority alsoEmpty2 = new MultiMasterAuthority(",");
    assertTrue(empty2.equals(alsoEmpty2));
    assertEquals(empty2.hashCode(), alsoEmpty2.hashCode());
    assertEquals(empty2.compareTo(alsoEmpty2), 0);
  }

  @Test
  public void notEqualEmptyNonEmpty() {
    MultiMasterAuthority nonEmpty = new MultiMasterAuthority("host1:port1,host2:port2");
    MultiMasterAuthority empty = new MultiMasterAuthority("");
    MultiMasterAuthority alsoEmpty = new MultiMasterAuthority(",");

    assertFalse(nonEmpty.equals(empty));
    assertNotEquals(nonEmpty.hashCode(), empty.hashCode());
    assertNotEquals(nonEmpty.compareTo(empty), 0);

    assertFalse(nonEmpty.equals(alsoEmpty));
    assertNotEquals(nonEmpty.hashCode(), alsoEmpty.hashCode());
    assertNotEquals(nonEmpty.compareTo(alsoEmpty), 0);
  }

  @Test
  public void notEqualEmptySegments() {
    MultiMasterAuthority empty = new MultiMasterAuthority("");
    MultiMasterAuthority seg1 = new MultiMasterAuthority(",");
    MultiMasterAuthority seg2 = new MultiMasterAuthority(",,");
    assertFalse(empty.equals(seg1));
    assertNotEquals(empty.compareTo(seg1), 0);
    assertFalse(seg1.equals(seg2));
    assertNotEquals(seg1.compareTo(seg2), 0);
    assertFalse(seg2.equals(empty));
    assertNotEquals(seg2.compareTo(empty), 0);
  }

  @Test
  public void notEqualsDuplicates() {
    MultiMasterAuthority normal = new MultiMasterAuthority("host1:port1");
    MultiMasterAuthority insane = new MultiMasterAuthority("host1:port1,host1:port1");
    assertFalse(normal.equals(insane));
    assertNotEquals(normal.compareTo(insane), 0);
  }
}
