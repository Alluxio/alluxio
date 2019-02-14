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

package alluxio.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

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
    assertEquals("/foo/bar/baz", output);
  }

  /**
   * Test for the {@link HadoopUtils#getPathWithoutScheme(Path)} method that contains the schema.
   */
  @Test
  public void testGetPathWithoutSchemaThatContainsSchema() {
    final Path path = new Path("file:///foo/bar/baz");

    final String output = HadoopUtils.getPathWithoutScheme(path);
    assertEquals("/foo/bar/baz", output);
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
    assertFalse("/foo/bar/baz".equals(output));
  }

  /**
   * Test for the {@link HadoopUtils#getPathWithoutScheme(Path)} method from an HDFS URI.
   */
  @Test
  public void testGetPathWithoutSchemaFromHDFSURI() {
    final Path path = new Path(URI.create("hdfs://localhost:1234/foo/bar/baz?please=dont&show=up"));

    final String output = HadoopUtils.getPathWithoutScheme(path);
    assertEquals("/foo/bar/baz", output);
  }

  @Test
  public void testGetPathWithoutSchemeFromZookeeperURI() {
    Path path
        = new Path(URI.create("alluxio://zk@host1:port1,host2:port2,host3:port3/foo/bar/baz"));
    String output = HadoopUtils.getPathWithoutScheme(path);
    assertEquals("/foo/bar/baz", output);
  }
}
