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

package alluxio.underfs;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import alluxio.conf.TachyonConf;

@RunWith(PowerMockRunner.class)
@PrepareForTest(UnderFileSystemCluster.class)
public class UnderFileSystemClusterTest {

  private static final String BASE_DIR = "/tmp";
  private static final TachyonConf TACHYON_CONF = new TachyonConf();
  private UnderFileSystemCluster mUnderFileSystemCluster;

  @Before
  public void before() {
    mUnderFileSystemCluster = PowerMockito.mock(UnderFileSystemCluster.class);
  }

  /**
   * Tests the getting an {@link UnderFileSystemCluster} when none is cached will create one, start
   * it, and register a shutdown hook for it.
   */
  @Test
  public void getTest() throws IOException {
    PowerMockito.spy(UnderFileSystemCluster.class);

    Mockito.when(UnderFileSystemCluster.getUnderFilesystemCluster(BASE_DIR,
        TACHYON_CONF)).thenReturn(mUnderFileSystemCluster);

    Whitebox.setInternalState(UnderFileSystemCluster.class, "sUnderFSCluster",
        (UnderFileSystemCluster) null);

    Mockito.when(mUnderFileSystemCluster.isStarted()).thenReturn(false);

    // execute test
    UnderFileSystemCluster.get(BASE_DIR, TACHYON_CONF);

    UnderFileSystemCluster underFSCluster = Whitebox.getInternalState(UnderFileSystemCluster
        .class, "sUnderFSCluster");

    Assert.assertSame(mUnderFileSystemCluster, underFSCluster);

    Mockito.verify(underFSCluster).start();
    Mockito.verify(underFSCluster).registerJVMOnExistHook();
  }

  /**
   * Tests that the {UnderFileSystemCluster{@link #readEOFReturnsNegativeTest()} method will return
   * true only when the cluster type is "alluxio.underfs.hdfs.LocalMiniDFSCluster".
   */
  @Test
  public void readEOFReturnsNegativeTest() {
    Whitebox.setInternalState(UnderFileSystemCluster.class, "sUnderFSClass",
            (String) null);
    Assert.assertFalse(UnderFileSystemCluster.readEOFReturnsNegative());

    Whitebox.setInternalState(UnderFileSystemCluster.class, "sUnderFSClass",
        "XXXX");
    Assert.assertFalse(UnderFileSystemCluster.readEOFReturnsNegative());

    Whitebox.setInternalState(UnderFileSystemCluster.class, "sUnderFSClass",
        "alluxio.underfs.hdfs.LocalMiniDFSCluster");
    Assert.assertTrue(UnderFileSystemCluster.readEOFReturnsNegative());
  }
}
