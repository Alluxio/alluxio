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

import org.junit.Test;
import org.junit.runners.model.Statement;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/**
 * Unit tests for {@link SystemErrRule}.
 */
public class SystemErrRuleTest {
  private static final ByteArrayOutputStream OUTPUT = new ByteArrayOutputStream();
  private static final PrintStream ORIGINAL_SYSTEM_ERR = System.err;

  private Statement mStatement = new Statement() {
    @Override
    public void evaluate() throws Throwable {
      System.err.println("2048");
      assertEquals("2048\n", OUTPUT.toString());
      OUTPUT.reset();

      System.err.println("1234");
      assertEquals("1234\n", OUTPUT.toString());
    }
  };

  @Test
  public void testSystemErrRule() throws Throwable {
    new SystemErrRule(OUTPUT).apply(mStatement, null).evaluate();
    assertEquals(System.err, ORIGINAL_SYSTEM_ERR);
  }
}
