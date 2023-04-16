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

package alluxio.job.plan.transform.format.orc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import alluxio.AlluxioURI;
import alluxio.job.plan.transform.format.TableRow;

import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class OrcReaderTest {

  @Test
  public void testOrcFile() throws IOException {
    String resourceName = "TestOrcFile.columnProjection.orc";
    final int expectedRows = 21000;

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    final OrcReader orcReader = OrcReader.create(new AlluxioURI("file://" + absolutePath));

    final TableRow row0 = orcReader.read();
    assertEquals(-1155869325L, row0.getColumn("int1"));
    assertEquals("bb2c72394b1ab9f8", new String((byte[]) row0.getColumn("string1")));

    final TableRow row1 = orcReader.read();
    assertEquals(431529176L, row1.getColumn("int1"));
    assertEquals("e6c5459001105f17", new String((byte[]) row1.getColumn("string1")));

    for (int i = 0; i < expectedRows - 2; i++) {
      assertNotNull(orcReader.read());
    }
    assertNull(orcReader.read());
  }
}
