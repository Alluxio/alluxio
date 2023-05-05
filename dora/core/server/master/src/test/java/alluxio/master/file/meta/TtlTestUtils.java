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

package alluxio.master.file.meta;

import alluxio.grpc.CreateFilePOptions;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.master.file.contexts.CreateFileContext;

public class TtlTestUtils {
  public static Inode createFileWithIdAndTtl(long id, long ttl) {
    return Inode.wrap(MutableInodeFile.create(id, 0, "ignored", 0,
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl)))));
  }

  public static Inode createDirectoryWithIdAndTtl(long id, long ttl) {
    return Inode.wrap(MutableInodeFile.create(id, 0, "ignored", 0,
        CreateFileContext.mergeFrom(CreateFilePOptions.newBuilder()
            .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder().setTtl(ttl)))));
  }
}
