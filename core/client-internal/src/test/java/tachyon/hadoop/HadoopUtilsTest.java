/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.hadoop;

import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the {@link HadoopUtils} class.
 */
public final class HadoopUtilsTest {

  /**
   * Test for the {@link HadoopUtils#getPathWithoutScheme(Path)} method.
   */
  @Test
  public void testGetPathWithoutSchema() {
    final Path path = new Path("/foo/bar/baz");

    final String output = HadoopUtils.getPathWithoutScheme(path);
    Assert.assertEquals("/foo/bar/baz", output);
  }

  /**
   * Test for the {@link HadoopUtils#getPathWithoutScheme(Path)} method that contains the schema.
   */
  @Test
  public void testGetPathWithoutSchemaThatContainsSchema() {
    final Path path = new Path("file:///foo/bar/baz");

    final String output = HadoopUtils.getPathWithoutScheme(path);
    Assert.assertEquals("/foo/bar/baz", output);
  }

  /**
   * This test doesn't work the way you might expect.
   *
   * If you take the URI.create("hdfs://localhost:1234/foo/bar/baz?please=dont&amp;show=up").getPath
   * it will return /foo/bar/baz. If you go through Hadoop's Path using {@link Path#Path(String)}
   * then Hadoop injects the query params into the path, so when you call toURI it gives a different
   * response.
   */
  @Test
  public void testGetPathWithoutSchemaFromHDFS() {
    final Path path = new Path("hdfs://localhost:1234/foo/bar/baz?please=dont&show=up");

    final String output = HadoopUtils.getPathWithoutScheme(path);
    Assert.assertFalse("/foo/bar/baz".equals(output));
  }

  /**
   * Test for the {@link HadoopUtils#getPathWithoutScheme(Path)} method from an HDFS URI.
   */
  @Test
  public void testGetPathWithoutSchemaFromHDFSURI() {
    final Path path = new Path(URI.create("hdfs://localhost:1234/foo/bar/baz?please=dont&show=up"));

    final String output = HadoopUtils.getPathWithoutScheme(path);
    Assert.assertEquals("/foo/bar/baz", output);
  }
}
