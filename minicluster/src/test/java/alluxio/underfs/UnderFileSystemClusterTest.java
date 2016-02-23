/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.underfs;

import alluxio.Configuration;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;

@RunWith(PowerMockRunner.class)
@PrepareForTest(UnderFileSystemCluster.class)
public class UnderFileSystemClusterTest {

  private static final String BASE_DIR = "/tmp";
  private static final Configuration ALLUXIO_CONF = new Configuration();
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
        ALLUXIO_CONF)).thenReturn(mUnderFileSystemCluster);

    Whitebox.setInternalState(UnderFileSystemCluster.class, "sUnderFSCluster",
        (UnderFileSystemCluster) null);

    Mockito.when(mUnderFileSystemCluster.isStarted()).thenReturn(false);

    // execute test
    UnderFileSystemCluster.get(BASE_DIR, ALLUXIO_CONF);

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
