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

public class MultiMasterAuthorityTest {
  @Test
  public void equalsPermutationsUnordered() {
    testPermutations(ImmutableList.of("host1:port1", "host2:port2"));
    testPermutations(ImmutableList.of("host1:port1", "host2:port2", "host3:port3"));
    testPermutations(ImmutableList.of("host1:port1", "longHostname:somePort", "localhost:19998"));
  }

  private void testPermutations(List<String> masters) {
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
      assertEquals(a1, a2);
      assertEquals(a1.hashCode(), a2.hashCode());
    }
  }

  @Test
  public void equalsSingleMaster() {
    MultiMasterAuthority a1 = new MultiMasterAuthority("host1:port1");
    MultiMasterAuthority a1Duplicate = new MultiMasterAuthority("host1:port1");
    assertEquals(a1, a1Duplicate);
    assertEquals(a1.hashCode(), a1Duplicate.hashCode());
  }

  @Test
  public void equalsEmpty() {
    MultiMasterAuthority empty = new MultiMasterAuthority("");
    MultiMasterAuthority alsoEmpty = new MultiMasterAuthority("");
    assertEquals(empty, alsoEmpty);
    assertEquals(empty.hashCode(), alsoEmpty.hashCode());

    MultiMasterAuthority empty2 = new MultiMasterAuthority(",");
    MultiMasterAuthority alsoEmpty2 = new MultiMasterAuthority(",");
    assertEquals(empty2, alsoEmpty2);
    assertEquals(empty2.hashCode(), alsoEmpty2.hashCode());
  }

  @Test
  public void notEqualDifferentHost() {
    MultiMasterAuthority a1 = new MultiMasterAuthority("host1:port1");
    MultiMasterAuthority a2 = new MultiMasterAuthority("host2:port1");
    assertNotEquals(a1, a2);
  }

  @Test
  public void notEqualDifferentNumberOfMasters() {
    MultiMasterAuthority oneMaster = new MultiMasterAuthority("host1:port1");
    MultiMasterAuthority twoMasters = new MultiMasterAuthority("host1:port1,host2:port2");
    assertNotEquals(oneMaster, twoMasters);
  }

  @Test
  public void notEqualDifferentHosts() {
    MultiMasterAuthority withHost2 = new MultiMasterAuthority("host1:port1,host2:port1");
    MultiMasterAuthority withHost3 = new MultiMasterAuthority("host1:port1,host3:port1");
    assertNotEquals(withHost2, withHost3);
  }

  @Test
  public void notEqualDifferentPorts() {
    MultiMasterAuthority withPort2 = new MultiMasterAuthority("host1:port1,host2:port2");
    MultiMasterAuthority withPort3 = new MultiMasterAuthority("host1:port1,host2:port3");
    assertNotEquals(withPort2, withPort3);
  }

  @Test
  public void notEqualEmptyNonEmpty() {
    MultiMasterAuthority nonEmpty = new MultiMasterAuthority("host1:port1,host2:port2");
    MultiMasterAuthority empty = new MultiMasterAuthority("");
    MultiMasterAuthority alsoEmpty = new MultiMasterAuthority(",");
    assertNotEquals(nonEmpty, empty);
    assertNotEquals(nonEmpty, alsoEmpty);
  }

  @Test
  public void notEqualEmptySegments() {
    MultiMasterAuthority empty = new MultiMasterAuthority("");
    MultiMasterAuthority seg1 = new MultiMasterAuthority(",");
    MultiMasterAuthority seg2 = new MultiMasterAuthority(",,");
    assertNotEquals(empty, seg1);
    assertNotEquals(seg1, seg2);
    assertNotEquals(seg2, empty);
  }

  @Test
  public void notEqualsDuplicates() {
    MultiMasterAuthority normal = new MultiMasterAuthority("host1:port1");
    MultiMasterAuthority insane = new MultiMasterAuthority("host1:port1,host1:port1");
    assertNotEquals(normal, insane);
  }
}
