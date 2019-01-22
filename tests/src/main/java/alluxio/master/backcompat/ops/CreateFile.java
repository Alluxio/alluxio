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
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.WritePType;
import alluxio.master.backcompat.FsTestOp;
import alluxio.master.backcompat.Utils;
import alluxio.security.authorization.Mode;
import alluxio.security.authorization.ModeParser;
import alluxio.grpc.TtlAction;

import java.util.Arrays;

/**
 * Test for file creation.
 */
public final class CreateFile extends FsTestOp {
  private static final AlluxioURI PATH = new AlluxioURI("/createFile");
  private static final AlluxioURI NESTED = new AlluxioURI("/createFileNested/a");
  private static final Mode TEST_MODE = ModeParser.parse("u=rw,g=x");
  private static final AlluxioURI MODE = new AlluxioURI("/createFileMode/a");
  private static final AlluxioURI THROUGH = new AlluxioURI("/createFileThrough/a");
  private static final long TEST_TTL = Long.MAX_VALUE / 2;
  private static final AlluxioURI TTL = new AlluxioURI("/createFileTtl/a");

  @Override
  public void apply(FileSystem fs) throws Exception {
    Utils.createFile(fs, PATH);
    Utils.createFile(fs, NESTED);
    Utils.createFile(fs, MODE, CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
        .setRecursive(true).setMode(TEST_MODE.toProto()).build());
    Utils.createFile(fs, THROUGH, CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB)
        .setRecursive(true).setWriteType(WritePType.THROUGH).build());
    Utils.createFile(fs, TTL,
        CreateFilePOptions.newBuilder().setBlockSizeBytes(Constants.KB).setRecursive(true)
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(TEST_TTL)
                .setTtlAction(alluxio.grpc.TtlAction.FREE))
            .build());
  }

  @Override
  public void check(FileSystem fs) throws Exception {
    for (AlluxioURI file : Arrays.asList(PATH, NESTED, THROUGH, TTL)) {
      assertTrue(fs.exists(file));
    }
    assertEquals(TEST_MODE, new Mode((short) fs.getStatus(MODE).getMode()));
    assertTrue(fs.getStatus(THROUGH).isPersisted());
    assertEquals(TEST_TTL, fs.getStatus(TTL).getTtl());
    assertEquals(TtlAction.FREE, fs.getStatus(TTL).getTtlAction());
  }
}
