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
import tachyon.conf.CommonConf;

/**
 * Unit tests for <code>tachyon.hadoop.GlusterFS</code>.
 */
public class GlusterFSTest {
  private UnderFileSystem mHcfs = null;
  private String mMount = null, mVolume = null, ufs = null;

  @After
  public final void after() throws Exception {
    if (mMount != null && !mMount.equals("") && 
        mVolume != null && !mVolume.equals("") &&
        ufs != null && ufs.equals("tachyon.GlusterfsCluster")) {
      System.clearProperty("fs.default.name");
      System.clearProperty("tachyon.underfs.glusterfs.mapred.system.dir");
      System.clearProperty("tachyon.underfs.glusterfs.mounts");
      System.clearProperty("tachyon.underfs.glusterfs.volumes");
    }
  }

  @Before
  public final void before() throws IOException {
    ufs = System.getProperty("ufs");
    mMount = CommonConf.get().UNDERFS_GLUSTERFS_MOUNTS;
    mVolume = CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES;
    if (mMount != null && !mMount.equals("") && 
        mVolume != null && !mVolume.equals("") &&
        ufs != null && ufs.equals("tachyon.GlusterfsCluster")) {
      System.setProperty("fs.default.name", "glusterfs:///");
    }
  }

  @Test
  public void createGlusterFS() throws Exception {
    if (mMount != null && !mMount.equals("") && 
        mVolume != null && !mVolume.equals("") &&
        ufs != null && ufs.equals("tachyon.GlusterfsCluster")) {
      mHcfs = UnderFileSystem.get("glusterfs:///");
      System.out.println("env " + mMount + " " + mVolume);
      Assert.assertTrue(mHcfs.create("tachyon_test") != null);
    }
  }
}
