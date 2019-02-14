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

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link InconsistentProperty}.
 */
public class InconsistentPropertyTest {

  @Test
  public void proto() {
    InconsistentProperty inconsistentProperty = createInconsistentProperty();
    InconsistentProperty other = inconsistentProperty.fromProto(inconsistentProperty.toProto());
    checkEquality(inconsistentProperty, other);
  }

  @Test
  public void testToString() {
    InconsistentProperty inconsistentProperty = createInconsistentProperty();
    String result = inconsistentProperty.toString();
    Assert.assertFalse(result.contains("Optional"));
    String expected = "InconsistentProperty{key=my_key, values="
        + "no value set (workerHostname1:workerPort, workerHostname2:workerPort), "
        + "some_value (masterHostname1:port, masterHostname2:port)}";
    Assert.assertEquals(expected, result);
  }

  /**
   * @return the created inconsistent property
   */
  private static InconsistentProperty createInconsistentProperty() {
    Map<Optional<String>, List<String>> values = new HashMap<>();
    values.put(Optional.ofNullable(null),
        Arrays.asList("workerHostname1:workerPort", "workerHostname2:workerPort"));
    values.put(Optional.of("some_value"),
        Arrays.asList("masterHostname1:port", "masterHostname2:port"));
    return new InconsistentProperty().setName("my_key").setValues(values);
  }

  /**
   * Checks if the two InconsistentProperty objects are equal.
   *
   * @param a the first InconsistentProperty object to be checked
   * @param b the second InconsistentProperty object to be checked
   */
  private void checkEquality(InconsistentProperty a, InconsistentProperty b) {
    Assert.assertEquals(a.getName(), b.getName());
    Assert.assertEquals(a.getValues(), b.getValues());
    Assert.assertEquals(a, b);
  }
}
