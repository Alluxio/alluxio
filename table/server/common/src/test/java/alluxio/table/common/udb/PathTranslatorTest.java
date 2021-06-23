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

package alluxio.table.common.udb;

import static org.junit.Assert.assertEquals;

import alluxio.util.io.PathUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class PathTranslatorTest {

  @Rule
  public ExpectedException mException = ExpectedException.none();

  private PathTranslator mTranslator;

  @Before
  public void before() throws Exception {
    mTranslator = new PathTranslator();
  }

  @Test
  public void noMapping() throws Exception {
    mTranslator.addMapping("alluxio:///my/table/directory", "ufs://a/the/ufs/location");
    mException.expect(IOException.class);
    mTranslator.toAlluxioPath("ufs://b/no/mapping");
  }

  @Test
  public void noMappingSingleLevel() throws Exception {
    mTranslator.addMapping("alluxio:///my/table/directory", "ufs://a/the/ufs/location");
    mException.expect(IOException.class);
    mTranslator.toAlluxioPath("ufs://a/the/ufs/no_loc");
  }

  @Test
  public void exactMatch() throws Exception {
    String alluxioPath  = "alluxio:///my/table/directory";
    String ufsPath = "ufs://a/the/ufs/location";
    mTranslator.addMapping(alluxioPath, ufsPath);
    assertEquals(alluxioPath, mTranslator.toAlluxioPath(ufsPath));
  }

  @Test
  public void singleSubDirectory() throws Exception {
    String alluxioPath  = "alluxio:///my/table/directory";
    String ufsPath = "ufs://a/the/ufs/location";
    mTranslator.addMapping(alluxioPath, ufsPath);
    assertEquals(PathUtils.concatPath(alluxioPath, "subdir"),
        mTranslator.toAlluxioPath(PathUtils.concatPath(ufsPath, "subdir")));
  }

  @Test
  public void multipleSubdir() throws Exception {
    String alluxioPath  = "alluxio:///my/table/directory";
    String ufsPath = "ufs://a/the/ufs/location";
    mTranslator.addMapping(alluxioPath, ufsPath);
    assertEquals(PathUtils.concatPath(alluxioPath, "subdir/a/b/c"),
        mTranslator.toAlluxioPath(PathUtils.concatPath(ufsPath, "subdir/a/b/c")));
  }

  @Test
  public void samePathDifferentUfs() throws Exception {
    String alluxioPath  = "alluxio:///my/table/directory";
    String ufsPath = "/the/ufs/location";
    mTranslator.addMapping(alluxioPath, "ufs_a://" + ufsPath);
    mException.expect(IOException.class);
    mTranslator.toAlluxioPath("ufs_b://" + ufsPath);
  }

  @Test
  public void noSchemeUfs() throws Exception {
    String alluxioPath  = "alluxio:///my/table/directory";
    String ufsPath = "/the/ufs/location";
    mTranslator.addMapping(alluxioPath, ufsPath);
    assertEquals(alluxioPath, mTranslator.toAlluxioPath(ufsPath));
  }

  @Test
  public void trailingSeparator() throws Exception {
    String alluxioPath  = "alluxio:///my/table/directory";
    String ufsPath = "/the/ufs/location";
    mTranslator.addMapping(alluxioPath + "/", ufsPath + "/");
    assertEquals(alluxioPath, mTranslator.toAlluxioPath(ufsPath));
    assertEquals(alluxioPath + "/", mTranslator.toAlluxioPath(ufsPath + "/"));
    mTranslator.addMapping(alluxioPath, ufsPath);
    assertEquals(alluxioPath, mTranslator.toAlluxioPath(ufsPath));
    assertEquals(alluxioPath + "/", mTranslator.toAlluxioPath(ufsPath + "/"));
  }
}
