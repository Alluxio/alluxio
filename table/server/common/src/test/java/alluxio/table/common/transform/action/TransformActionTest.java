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

package alluxio.table.common.transform.action;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class TransformActionTest {

  @Test
  public void parse() {
    List<String> definitions = Arrays.asList(
        "write(type)",
        " write(type) ",
        " write( t ) ",
        "write(t).option(some.opt1, 100).option(some.opt2, 2gb)",
        "write(t) .option(opt1 , 100) .option(opt2 , 2gb )",
        "write(t).option(opt1, 10.0)",
        "write(t)\n\t  .option(opt1, 10.0)",
        "write(\n\t  t)"
    );

    parseValidInternal(definitions);
  }

  @Test
  public void parseInvalid() {
    List<String> definitions = Arrays.asList(
        "doesNotExist()",
        "write(t)..option(some.option1, 100)",
        "write(t).option()",
        "write(t).option(1, 2, 3)",
        "write(t).write()",
        "write(t). option(a, b)",
        "write (t)",
        "wri te(t)",
        "write(t);"
    );
    parseInvalidInternal(definitions);
  }

  private void parseValidInternal(List<String> definitions) {
    for (String definition : definitions) {
      assertNotNull("Should be parsable: " + definition, TransformAction.Parser.parse(definition));
    }
  }

  private void parseInvalidInternal(List<String> definitions) {
    for (String definition : definitions) {
      try {
        TransformAction.Parser.parse(definition);
        fail("Should not be parsable: " + definition);
      } catch (Exception e) {
        // ignore
      }
    }
  }
}
