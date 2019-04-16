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

package alluxio.master.backcompat.ops;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.grpc.CreateDirectoryPOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.TtlAction;
import alluxio.grpc.WritePType;
import alluxio.master.backcompat.FsTestOp;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.ModeParser;

import java.util.Arrays;

/**
 * Test for directory creation.
 */
public final class CreateDirectory extends FsTestOp {
  private static final AlluxioURI DIR = new AlluxioURI("/createDirectory");
  private static final AlluxioURI NESTED_DIR = new AlluxioURI("/createDirectory/a");
  private static final AlluxioURI NESTED_NESTED_DIR = new AlluxioURI("/createDirectory/a/b");
  private static final AlluxioURI RECURSIVE = new AlluxioURI("/createDirectoryRecursive/a/b");
  private static final Mode TEST_MODE = ModeParser.parse("u=rwx,g=x,o=wx");
  private static final AlluxioURI MODE_DIR = new AlluxioURI("/createDirectoryMode/a");
  private static final Long TTL = Long.MAX_VALUE / 2;
  private static final AlluxioURI TTL_DIR = new AlluxioURI("/createDirectoryTtl/a");
  private static final AlluxioURI COMMON_TTL_DIR = new AlluxioURI("/createDirectoryCommonTtl/a");
  private static final AlluxioURI THROUGH_DIR = new AlluxioURI("/createDirectoryThrough/a");
  private static final AlluxioURI ALL_OPTS_DIR = new AlluxioURI("/createDirectoryAllOpts/a");

  @Override
  public void apply(FileSystem fs) throws Exception {
    fs.createDirectory(DIR);
    fs.createDirectory(NESTED_DIR);
    fs.createDirectory(NESTED_NESTED_DIR);
    fs.createDirectory(RECURSIVE, CreateDirectoryPOptions.newBuilder().setRecursive(true).build());
    fs.createDirectory(RECURSIVE,
        CreateDirectoryPOptions.newBuilder().setAllowExists(true).build());
    fs.createDirectory(MODE_DIR, CreateDirectoryPOptions.newBuilder().setMode(TEST_MODE.toProto())
        .setRecursive(true).build());
    // Set TTL via common options instead (should have the same effect).
    fs.createDirectory(COMMON_TTL_DIR,
        CreateDirectoryPOptions.newBuilder().setRecursive(true).setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setTtl(TTL).setTtlAction(TtlAction.DELETE))
            .build());
    fs.createDirectory(TTL_DIR,
        CreateDirectoryPOptions.newBuilder().setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setTtl(TTL).setTtlAction(TtlAction.DELETE))
            .setRecursive(true).build());
    fs.createDirectory(THROUGH_DIR, CreateDirectoryPOptions.newBuilder()
        .setWriteType(WritePType.THROUGH).setRecursive(true).build());
    fs.createDirectory(ALL_OPTS_DIR, CreateDirectoryPOptions.newBuilder().setRecursive(true)
        .setMode(TEST_MODE.toProto()).setAllowExists(true).setWriteType(WritePType.THROUGH)
        .setCommonOptions(
            FileSystemMasterCommonPOptions.newBuilder().setTtl(TTL).setTtlAction(TtlAction.DELETE))
        .build());
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    for (AlluxioURI dir : Arrays.asList(DIR, NESTED_DIR, NESTED_NESTED_DIR, RECURSIVE, MODE_DIR,
        TTL_DIR, COMMON_TTL_DIR, THROUGH_DIR, ALL_OPTS_DIR)) {
      assertTrue(fs.exists(dir));
    }
    assertEquals(TEST_MODE, new Mode((short) fs.getStatus(MODE_DIR).getMode()));
    assertEquals((long) TTL, fs.getStatus(TTL_DIR).getTtl());
    assertEquals(alluxio.grpc.TtlAction.DELETE, fs.getStatus(TTL_DIR).getTtlAction());
    assertEquals((long) TTL, fs.getStatus(COMMON_TTL_DIR).getTtl());
    assertEquals(alluxio.grpc.TtlAction.DELETE, fs.getStatus(COMMON_TTL_DIR).getTtlAction());
    assertTrue(fs.getStatus(THROUGH_DIR).isPersisted());
    assertEquals(TEST_MODE, new Mode((short) fs.getStatus(ALL_OPTS_DIR).getMode()));
    assertEquals((long) TTL, fs.getStatus(ALL_OPTS_DIR).getTtl());
    assertEquals(alluxio.grpc.TtlAction.DELETE, fs.getStatus(ALL_OPTS_DIR).getTtlAction());
    assertTrue(fs.getStatus(ALL_OPTS_DIR).isPersisted());
  }
}
