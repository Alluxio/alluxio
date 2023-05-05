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

package alluxio.table.common.transform;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TransformDefinitionTest {

  @Test
  public void parse() {
    List<String> definitions = Arrays.asList(
        "file.count.max=2",
        "  file.count.max=2  ",
        "  file.count.max:2",
        "file.count.max:2;",
        "file.count.max:2\nfile.count.max:2"
    );

    parseValidInternal(definitions);
  }

  @Test
  public void parseInvalid() {
    List<String> definitions = Arrays.asList(
        "file.count.max:",
        "file.does.not.exist:2",
        ""
    );
    parseInvalidInternal(definitions);
  }

  private void parseValidInternal(List<String> definitions) {
    for (String definition : definitions) {
      final TransformDefinition transformDefinition = TransformDefinition.parse(definition);
      assertNotNull("Should be parsable: " + definition, transformDefinition);
      assertEquals("Should be parsable: " + definition,
          1, transformDefinition.getActions().size());
    }
  }

  private void parseInvalidInternal(List<String> definitions) {
    for (String definition : definitions) {
      TransformDefinition transformDefinition = null;
      try {
        transformDefinition = TransformDefinition.parse(definition);
      } catch (Exception e) {
        // ignore
      }
      if (transformDefinition != null && !transformDefinition.getActions().isEmpty()) {
        fail("Should not be parsable: " + definition);
      }
    }
  }
}
