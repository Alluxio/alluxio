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

package alluxio.underfs;

import alluxio.AlluxioURI;
import alluxio.collections.Pair;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

/**
 * Unit tests for {@link UnderFileSystem}.
 */
public final class UnderFileSystemTest {
  /**
   * Tests the {@link UnderFileSystem#parse(AlluxioURI)} method.
   */
  @Test
  public void parse() {
    Pair<String, String> result = UnderFileSystem.parse(new AlluxioURI("/path"));
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("file:///path"));
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("alluxio://localhost:19998"));
    Assert.assertEquals(result.getFirst(), "alluxio://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new AlluxioURI("alluxio://localhost:19998/"));
    Assert.assertEquals(result.getFirst(), "alluxio://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new AlluxioURI("alluxio://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "alluxio://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("alluxio-ft://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "alluxio-ft://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("hdfs://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "hdfs://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("s3://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "s3://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("s3a://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "s3a://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("s3n://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "s3n://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new AlluxioURI("oss://localhost:19998"));
    Assert.assertEquals(result.getFirst(), "oss://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new AlluxioURI("oss://localhost:19998/"));
    Assert.assertEquals(result.getFirst(), "oss://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new AlluxioURI("oss://localhost:19998/path"));
    Assert.assertEquals(result.getFirst(), "oss://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    Assert.assertEquals(UnderFileSystem.parse(AlluxioURI.EMPTY_URI), null);
    Assert.assertEquals(UnderFileSystem.parse(new AlluxioURI("anythingElse")), null);
  }

  /**
   * Tests the {@link UnderFileSystemRegistry#find(String)} method when using a core
   * factory.
   */
  @Test
  public void coreFactory() {
    // Supported in core
    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("/test/path");
    Assert.assertNull("An UnderFileSystemFactory should not exist for local file paths", factory);

    factory = UnderFileSystemRegistry.find("file:///test/path");
    Assert.assertNull("An UnderFileSystemFactory should not exist for local file paths", factory);
  }

  /**
   * Tests the {@link UnderFileSystemRegistry#find(String)} method when using an
   * external factory.
   */
  @Test
  public void externalFactory() {
    // As we are going to use some Maven trickery to re-use the test cases as is in the external
    // modules this test needs to assume that only the core implementations are present as otherwise
    // when we try and run it in the external modules it will fail
    // In core there is only one under file system implementation, if there are any more we aren't
    // running in core
    Assume.assumeTrue(UnderFileSystemRegistry.available().size() == 1);

    // Requires additional modules
    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("hdfs://localhost/test/path");
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for HDFS paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("oss://localhost/test/path");
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for OSS paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("s3://localhost/test/path");
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for S3 paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("s3n://localhost/test/path");
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for S3 paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("glusterfs://localhost/test/path");
    Assert.assertNull("No UnderFileSystemFactory should exist for Gluster FS paths as it requires"
        + " a separate module", factory);
  }
}
