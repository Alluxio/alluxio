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

package alluxio.underfs.web;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.underfs.UnderFileSystem;
import alluxio.util.ConfigurationUtils;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Unit tests for the {@link WebUnderFileSystem}.
 */
public class WebUnderFileSystemTest {
  private String mWebUfsRoot;
  private UnderFileSystem mWebUfs;
  private static AlluxioConfiguration sConf =
      new InstancedConfiguration(ConfigurationUtils.defaults());

  @Before
  public void before() throws IOException {
    mWebUfsRoot = "https://archive.apache.org/dist/";
    mWebUfs = UnderFileSystem.Factory.create(mWebUfsRoot, sConf);
  }

  @Test
  public void exists() throws IOException {
    assertTrue(mWebUfs.exists(mWebUfsRoot));
  }

  @Test
  public void isDirectory() throws IOException {
    assertTrue(mWebUfs.isDirectory(mWebUfsRoot));
  }

  @Test
  public void isFile() throws IOException {
    assertFalse(mWebUfs.isFile(mWebUfsRoot));
  }
}
