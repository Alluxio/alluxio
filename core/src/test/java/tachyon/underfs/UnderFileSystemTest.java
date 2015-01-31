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
import org.junit.Before;
import org.junit.Test;
import tachyon.conf.TachyonConf;

import tachyon.Pair;
import tachyon.TachyonURI;
import tachyon.underfs.UnderFileSystem;

/**
 * Unit tests for {@link UnderFileSystem}
 */
public final class UnderFileSystemTest {
  private TachyonConf mTachyonConf;

  @Before
  public void before() {
    mTachyonConf = new TachyonConf();
  }

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

    Assert.assertEquals(UnderFileSystem.parse(TachyonURI.EMPTY_URI, mTachyonConf), null);
    Assert.assertEquals(UnderFileSystem.parse(new TachyonURI("anythingElse"), mTachyonConf), null);
  }
  
  @Test
  public void factoryTest() {
    // Supported in core
    UnderFileSystemFactory factory = UnderFileSystemRegistry.find("/test/path");
    Assert.assertNotNull("A UnderFileSystemFactory should exist for local file paths", factory);
    
    factory = UnderFileSystemRegistry.find("file:///test/path");
    Assert.assertNotNull("A UnderFileSystemFactory should exist for local file paths", factory);
    
    // Requires additional modules
    factory = UnderFileSystemRegistry.find("hdfs://localhost/test/path");
    Assert.assertNull("No UnderFileSystemFactory should exist for HDFS paths as it requires a separate module", factory);
    
    factory = UnderFileSystemRegistry.find("s3://localhost/test/path");
    Assert.assertNull("No UnderFileSystemFactory should exist for S3 paths as it requires a separate module", factory);
    
    factory = UnderFileSystemRegistry.find("s3n://localhost/test/path");
    Assert.assertNull("No UnderFileSystemFactory should exist for S3 paths as it requires a separate module", factory);
    
    factory = UnderFileSystemRegistry.find("glusterfs://localhost/test/path");
    Assert.assertNull("No UnderFileSystemFactory should exist for Gluster FS paths as it requires a separate module", factory);
  }
}
