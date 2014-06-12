/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.hadoop;

import java.io.IOException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.UnderFileSystem;

/**
 * Unit tests for <code>tachyon.hadoop.GlusterFS</code>.
 */
public class GlusterFSTest {
  private UnderFileSystem mHcfs = null;
  private String mMount = null;
  private static final String GLUSTERFS_MOUNT = "GlusterfsMount";

  @After
  public final void after() throws Exception {
    if (mMount != null) {
      System.clearProperty("fs.default.name");
      System.clearProperty("tachyon.underfs.glusterfs.mapred.system.dir");
      System.clearProperty("tachyon.underfs.glusterfs.mounts");
      System.clearProperty("tachyon.underfs.glusterfs.volumes");
    }
  }

  @Before
  public final void before() throws IOException {
    mMount = System.getProperty(GLUSTERFS_MOUNT);
    if (mMount != null) {
      System.setProperty("fs.default.name", "glusterfs:///");
      System.setProperty("tachyon.underfs.glusterfs.mapred.system.dir",
          "glusterfs:///mapred/system");
      System.setProperty("tachyon.underfs.glusterfs.mounts", mMount);
      System.setProperty("tachyon.underfs.glusterfs.volumes", "tachyon_vol");
    }
  }

  @Test
  public void createGlusterFS() throws Exception {
    if (mMount != null) {
      mHcfs = UnderFileSystem.get("glusterfs:///");
      Assert.assertTrue(mHcfs.create("tachyon_test") != null);
    }
  }
}
