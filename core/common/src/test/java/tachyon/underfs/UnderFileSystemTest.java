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

package tachyon.underfs;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import tachyon.collections.Pair;
import tachyon.TachyonURI;
import tachyon.conf.TachyonConf;

/**
 * Unit tests for {@link UnderFileSystem}
 */
public final class UnderFileSystemTest {
  private TachyonConf mTachyonConf;

  /**
   * Sets up the configuration for Tachyon before running a test.
   */
  @Before
  public void before() {
    mTachyonConf = new TachyonConf();
  }

  /**
   * Tests the {@link UnderFileSystem#parse(TachyonURI, TachyonConf)} method.
   */
  @Test
  public void parseTest() {
    Pair<String, String> result = UnderFileSystem.parse(new TachyonURI("/path"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("file:///path"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "/");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("tachyon://localhost:19998"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new TachyonURI("tachyon://localhost:19998/"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new TachyonURI("tachyon://localhost:19998/path"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "tachyon://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("tachyon-ft://localhost:19998/path"),
        mTachyonConf);
    Assert.assertEquals(result.getFirst(), "tachyon-ft://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("hdfs://localhost:19998/path"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "hdfs://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("s3://localhost:19998/path"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "s3://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("s3n://localhost:19998/path"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "s3n://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    result = UnderFileSystem.parse(new TachyonURI("oss://localhost:19998"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "oss://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new TachyonURI("oss://localhost:19998/"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "oss://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/");

    result = UnderFileSystem.parse(new TachyonURI("oss://localhost:19998/path"), mTachyonConf);
    Assert.assertEquals(result.getFirst(), "oss://localhost:19998");
    Assert.assertEquals(result.getSecond(), "/path");

    Assert.assertEquals(UnderFileSystem.parse(TachyonURI.EMPTY_URI, mTachyonConf), null);
    Assert.assertEquals(UnderFileSystem.parse(new TachyonURI("anythingElse"), mTachyonConf), null);
  }

  /**
   * Tests the {@link UnderFileSystemRegistry#find(String, TachyonConf)} method when using a core
   * factory.
   */
  @Test
  public void coreFactoryTest() {
    // Supported in core
    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("/test/path", mTachyonConf);
    Assert.assertNull("An UnderFileSystemFactory should not exist for local file paths", factory);

    factory = UnderFileSystemRegistry.find("file:///test/path", mTachyonConf);
    Assert.assertNull("An UnderFileSystemFactory should not exist for local file paths", factory);
  }

  /**
   * Tests the {@link UnderFileSystemRegistry#find(String, TachyonConf)} method when using an
   * external factory.
   */
  @Test
  public void externalFactoryTest() {
    // As we are going to use some Maven trickery to re-use the test cases as is in the external
    // modules this test needs to assume that only the core implementations are present as otherwise
    // when we try and run it in the external modules it will fail
    // In core there is only one under file system implementation, if there are any more we aren't
    // running in core
    Assume.assumeTrue(UnderFileSystemRegistry.available().size() == 1);

    // Requires additional modules
    UnderFileSystemFactory factory =
        UnderFileSystemRegistry.find("hdfs://localhost/test/path", mTachyonConf);
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for HDFS paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("oss://localhost/test/path", mTachyonConf);
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for OSS paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("s3://localhost/test/path", mTachyonConf);
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for S3 paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("s3n://localhost/test/path", mTachyonConf);
    Assert.assertNull(
        "No UnderFileSystemFactory should exist for S3 paths as it requires a separate module",
        factory);

    factory = UnderFileSystemRegistry.find("glusterfs://localhost/test/path", mTachyonConf);
    Assert.assertNull("No UnderFileSystemFactory should exist for Gluster FS paths as it requires"
        + " a separate module", factory);
  }
}
