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

package alluxio.underfs.cephfs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConfiguration;

import com.ceph.fs.CephMount;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.mockito.Mockito;

/**
 * Tests {@link CephFSUnderFileSystem}.
 */
public final class CephFSUnderFileSystemTest {

  private CephFSUnderFileSystem mCephFSUnderFileSystem;
  private CephMount mMount;

  @Before
  public final void before() throws Exception {
    mMount = Mockito.mock(CephMount.class);
    mCephFSUnderFileSystem = new CephFSUnderFileSystem(new AlluxioURI("cephfs:///"),
        mMount, UnderFileSystemConfiguration.defaults());
  }

  /**
   * Tests the {@link CephFSUnderFileSystem#getUnderFSType()} method. Confirm the UnderFSType for
   * CephFSUnderFileSystem.
   */
  @Test
  public void getUnderFSType() throws Exception {
    Assert.assertEquals("cephfs", mCephFSUnderFileSystem.getUnderFSType());
  }
}
