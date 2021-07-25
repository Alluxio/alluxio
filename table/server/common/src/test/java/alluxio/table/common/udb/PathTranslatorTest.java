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

import alluxio.ConfigurationRule;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.util.io.PathUtils;

import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class PathTranslatorTest {
  private static final String MASTER_HOSTNAME = "master";
  private static final String MASTER_RPC_PORT = "11111";
  private static final String ALLUXIO_URI_AUTHORITY = "alluxio://" + MASTER_HOSTNAME;
  private static final String ALLUXIO_URI_AUTHORITY_WITH_PORT =
      ALLUXIO_URI_AUTHORITY + ":" + MASTER_RPC_PORT;

  @Rule
  public ExpectedException mException = ExpectedException.none();

  @Rule
  public ConfigurationRule mConfiguration =
      new ConfigurationRule(
          ImmutableMap.of(PropertyKey.MASTER_HOSTNAME, MASTER_HOSTNAME,
            PropertyKey.MASTER_RPC_PORT, MASTER_RPC_PORT),
          ServerConfiguration.global());

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
    mTranslator.addMapping(alluxioPath, "ufs-a://" + ufsPath);
    mException.expect(IOException.class);
    mTranslator.toAlluxioPath("ufs-b://" + ufsPath);
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

  @Test
  public void prefixBoundaryWithinPathComponent() throws Exception {
    mTranslator.addMapping("alluxio:///table_a", "ufs://a/table1");
    mTranslator.addMapping("alluxio:///table_b", "ufs://a/table11");
    assertEquals("alluxio:///table_b/part1",
        mTranslator.toAlluxioPath("ufs://a/table11/part1"));
    assertEquals("alluxio:///table_a/part1",
        mTranslator.toAlluxioPath("ufs://a/table1/part1"));
  }

  @Test
  public void deepestMatch() throws Exception {
    mTranslator.addMapping("alluxio:///db1/tables/table1", "ufs://a/db1/table1");
    mTranslator.addMapping("alluxio:///db1/fragments/a", "ufs://a/db1");
    mTranslator.addMapping("alluxio:///db1/fragments/b", "ufs://b/db1");
    assertEquals("alluxio:///db1/tables/table1/part1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/part1"));
    assertEquals("alluxio:///db1/fragments/b/table1/part1",
        mTranslator.toAlluxioPath("ufs://b/db1/table1/part1"));
    assertEquals("alluxio:///db1/fragments/a/table2/part1",
        mTranslator.toAlluxioPath("ufs://a/db1/table2/part1"));
  }

  @Test
  public void alluxioUriWithSchemeOnly() throws Exception {
    mTranslator.addMapping("alluxio:///db1/tables/table1", "ufs://a/db1/table1");
    // non-exact match
    assertEquals("alluxio:///db1/tables/table1/part1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/part1"));
    // exact match
    assertEquals("alluxio:///db1/tables/table1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1"));
    // trailing slash is preserved
    assertEquals("alluxio:///db1/tables/table1/",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/"));
  }

  @Test
  public void alluxioUriWithSchemeAndAuthority() throws Exception {
    mTranslator.addMapping(
        ALLUXIO_URI_AUTHORITY + "/db1/tables/table1", "ufs://a/db1/table1");
    // non-exact match
    assertEquals(ALLUXIO_URI_AUTHORITY + "/db1/tables/table1/part1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/part1"));
    // exact match
    assertEquals(ALLUXIO_URI_AUTHORITY + "/db1/tables/table1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1"));
    // trailing slash is preserved
    assertEquals(ALLUXIO_URI_AUTHORITY + "/db1/tables/table1/",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/"));
  }

  @Test
  public void alluxioUriPurePath() throws Exception {
    mTranslator.addMapping("/db1/tables/table1", "ufs://a/db1/table1");
    // non-exact match
    assertEquals(ALLUXIO_URI_AUTHORITY_WITH_PORT + "/db1/tables/table1/part1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/part1"));
    // exact match
    assertEquals(ALLUXIO_URI_AUTHORITY_WITH_PORT + "/db1/tables/table1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1"));
    // trailing slash is preserved
    assertEquals(ALLUXIO_URI_AUTHORITY_WITH_PORT + "/db1/tables/table1/",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/"));
  }

  @Test
  public void bypassedUfsUri() throws Exception {
    mTranslator.addMapping("ufs://a/db1/table1", "ufs://a/db1/table1");
    assertEquals("ufs://a/db1/table1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1"));
    assertEquals("ufs://a/db1/table1/part1",
        mTranslator.toAlluxioPath("ufs://a/db1/table1/part1"));
  }

  @Test
  public void bypassedUfsPath() throws Exception {
    mTranslator.addMapping("/a/db1/table1", "/a/db1/table1");
    assertEquals("/a/db1/table1",
        mTranslator.toAlluxioPath("/a/db1/table1"));
    assertEquals("/a/db1/table1/part1",
        mTranslator.toAlluxioPath("/a/db1/table1/part1"));
  }
}
