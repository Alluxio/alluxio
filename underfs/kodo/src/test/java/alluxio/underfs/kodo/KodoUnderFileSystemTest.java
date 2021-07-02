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

package alluxio.underfs.kodo;

import alluxio.AlluxioURI;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.options.DeleteOptions;

import com.qiniu.common.QiniuException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * Unit tests for the {@link KodoUnderFileSystem}.
 */
public class KodoUnderFileSystemTest {

  private static InstancedConfiguration sConf = ConfigurationTestUtils.defaults();

  private KodoUnderFileSystem mKodoUnderFileSystem;
  private KodoClient mClient;

  private static final String PATH = "path";
  private static final String SRC = "src";
  private static final String DST = "dst";

  /**
   * Set up.
   */
  @Before
  public void before() throws InterruptedException {
    mClient = Mockito.mock(KodoClient.class);

    mKodoUnderFileSystem = new KodoUnderFileSystem(new AlluxioURI(""), mClient,
        UnderFileSystemConfiguration.defaults(sConf));
  }

  /**
   * Test case for {@link KodoUnderFileSystem#deleteDirectory(String, DeleteOptions)}.
   */
  @Test
  public void deleteNonRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listFiles(Matchers.anyString(), Matchers.anyString(), Matchers.anyInt(),
        Matchers.eq(null))).thenThrow(QiniuException.class);

    boolean result =
        mKodoUnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(false));

    Assert.assertFalse(result);
  }

  @Test
  public void deleteRecursiveOnServiceException() throws IOException {
    Mockito.when(mClient.listFiles(Matchers.anyString(), Matchers.anyString(), Matchers.anyInt(),
        Matchers.eq(null))).thenThrow(QiniuException.class);
    boolean result =
        mKodoUnderFileSystem.deleteDirectory(PATH, DeleteOptions.defaults().setRecursive(true));
    Assert.assertFalse(result);
  }

  /**
   * Test case for {@link KodoUnderFileSystem#renameFile(String, String)}.
   */
  @Test
  public void renameOnServiceException() throws IOException {
    Mockito.when(mClient.listFiles(Matchers.anyString(), Matchers.anyString(), Matchers.anyInt(),
        Matchers.eq(null))).thenThrow(QiniuException.class);

    boolean result = mKodoUnderFileSystem.renameFile(SRC, DST);
    Assert.assertFalse(result);
  }
}

