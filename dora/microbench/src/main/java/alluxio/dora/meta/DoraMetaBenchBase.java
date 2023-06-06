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

package alluxio.dora.meta;

import alluxio.AlluxioTestDirectory;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.FileInfo;
import alluxio.grpc.PAcl;
import alluxio.proto.meta.DoraMeta;
import alluxio.worker.dora.DoraMetaStore;
import alluxio.worker.dora.RocksDBDoraMetaStore;

import java.io.IOException;

class DoraMetaBenchBase {
  public static final String ROCKS = "rocks";
  public static final String ROCKS_1GB_CACHE = "rocks-1gb-cache";
  private static final boolean BOOLEAN_FIELD = false;
  private static final int INT_FIELD = 1;
  private static final String STRING_FIELD = "foobar";
  public static final String UFS_PATH_PREFIX = "s3://foo/bar/baz/qux/";

  public static DoraMeta.FileStatus makeFileStatus() {
    return DoraMeta.FileStatus.newBuilder()
        .setFileInfo(
            FileInfo.newBuilder()
                .setUfsFingerprint(STRING_FIELD)
                .setGroup(STRING_FIELD)
                .setCacheable(BOOLEAN_FIELD)
                .setCompleted(BOOLEAN_FIELD)
                .setBlockSizeBytes(INT_FIELD)
                .addBlockIds(INT_FIELD)
                .addBlockIds(INT_FIELD)
                .addBlockIds(INT_FIELD)
                .addBlockIds(INT_FIELD)
                .setAcl(PAcl.newBuilder()
                    .setMode(INT_FIELD)
                    .setOwner(STRING_FIELD)
                    .setOwningGroup(STRING_FIELD)
                    .setIsDefault(BOOLEAN_FIELD)
                    .build())
        ).setTs(INT_FIELD).build();
  }

  public static final DoraMeta.FileStatus FILE_STATUS = DoraMeta.FileStatus.newBuilder()
      .setFileInfo(
          FileInfo.newBuilder()
              .setUfsFingerprint(STRING_FIELD)
              .setGroup(STRING_FIELD)
              .setCacheable(BOOLEAN_FIELD)
              .setCompleted(BOOLEAN_FIELD)
              .setBlockSizeBytes(INT_FIELD)
              .addBlockIds(INT_FIELD)
              .addBlockIds(INT_FIELD)
              .addBlockIds(INT_FIELD)
              .addBlockIds(INT_FIELD)
              .setAcl(PAcl.newBuilder()
                  .setMode(INT_FIELD)
                  .setOwner(STRING_FIELD)
                  .setOwningGroup(STRING_FIELD)
                  .setIsDefault(BOOLEAN_FIELD)
                  .build())
      ).setTs(INT_FIELD).build();

  private final DoraMetaStore mDoraMetaStore;

  public DoraMetaStore getDoraMetaStore() {
    return mDoraMetaStore;
  }

  DoraMetaBenchBase(String inodeStoreType) throws Exception {
    mDoraMetaStore = getMetastore(inodeStoreType);
  }

  public void after() throws Exception {
  }

  static DoraMetaStore getMetastore(String type) throws IOException {
    String dir =
        AlluxioTestDirectory.createTemporaryDirectory("dora-metastore-bench")
            .getAbsolutePath();
    switch (type) {
      case ROCKS:
        return new RocksDBDoraMetaStore(dir, -1);
      case ROCKS_1GB_CACHE:
        Configuration.set(PropertyKey.DORA_WORKER_METASTORE_ROCKSDB_CACHE_SIZE,
            (long) 1024 * 1204 * 1024); // 1GB
        return new RocksDBDoraMetaStore(dir, -1);
      default:
        throw new IllegalStateException("Invalid type: " + type);
    }
  }
}
