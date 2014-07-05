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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import tachyon.UnderFileSystem;
import tachyon.conf.CommonConf;

/**
 * Unit tests for <code>tachyon.hadoop.GlusterFS</code>.
 */
public class GlusterFSTest {
  private UnderFileSystem mGfs = null;
  private String mMount = null;
  private String mVolume = null;

  @Before
  public final void before() throws IOException {
    mMount = CommonConf.get().UNDERFS_GLUSTERFS_MOUNTS;
    mVolume = CommonConf.get().UNDERFS_GLUSTERFS_VOLUMES;
  }

  @Test
  public void createGlusterFS() throws Exception {
    if (mMount != null && !mMount.equals("") && mVolume != null && !mVolume.equals("")) {
      mGfs = UnderFileSystem.get("glusterfs:///");
      Assert.assertTrue(mGfs.create("tachyon_test") != null);
    }
  }
}
