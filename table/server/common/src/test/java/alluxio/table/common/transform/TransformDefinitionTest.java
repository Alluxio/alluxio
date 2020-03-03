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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TransformDefinitionTest {

  @Test
  public void parse() {
    List<String> definitions = Arrays.asList(
        "write(type)",
        " write(type) ",
        "write(type);",
        "write(type); write(type)",
        "write(type); write(type);"
    );

    parseValidInternal(definitions);

    TransformDefinition.parse("write(hive).option(some.option1, 100).option(some.option2, 2gb);");
  }

  @Test
  public void parseInvalid() {
    List<String> definitions = Arrays.asList(
        "write(); dne()",
        "write();; write()",
        "write();; write();;",
        "write()  write()",
        "doesNotExist()"
    );
    parseInvalidInternal(definitions);
  }

  private void parseValidInternal(List<String> definitions) {
    for (String definition : definitions) {
      assertNotNull("Should be parsable: " + definition, TransformDefinition.parse(definition));
    }
  }

  private void parseInvalidInternal(List<String> definitions) {
    for (String definition : definitions) {
      try {
        TransformDefinition.parse(definition);
        fail("Should not be parsable: " + definition);
      } catch (Exception e) {
        // ignore
      }
    }
  }
}
