/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */
package alluxio.client.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.grpc.*;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.LoadMetadataType;
import alluxio.wire.TtlAction;

public final class FileSystemClientOptions {

  public static FileSystemMasterCommonPOptions getCommonOptions() {
    return FileSystemMasterCommonPOptions.newBuilder().setTtl(Constants.NO_TTL)
        .setTtlAction(alluxio.grpc.TtlAction.DELETE)
        .setSyncIntervalMs(Configuration.getMs(PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL))
        .build();
  }

  public static GetStatusPOptions getGetStatusOptions() {
    return GetStatusPOptions.newBuilder().setCommonOptions(getCommonOptions())
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  public static ListStatusPOptions getListStatusOptions() {
    FileSystemMasterCommonPOptions commonOptions =
        getCommonOptions().toBuilder().setTtl(Configuration.getMs(PropertyKey.USER_FILE_LOAD_TTL))
            .setTtlAction(GrpcUtils.toProto(
                Configuration.getEnum(PropertyKey.USER_FILE_LOAD_TTL_ACTION, TtlAction.class)))
            .build();

    return ListStatusPOptions.newBuilder().setCommonOptions(commonOptions)
        .setLoadMetadataType(GrpcUtils.toProto(Configuration
            .getEnum(PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class)))
        .build();
  }

  public static LoadMetadataPOptions getLoadMetadataOptions() {
    return LoadMetadataPOptions.newBuilder().setCommonOptions(getCommonOptions())
            .setRecursive(false)
            .build();
  }

  public static DeletePOptions getDeleteOptions() {
    return DeletePOptions.newBuilder().setCommonOptions(getCommonOptions())
            .setRecursive(false)
            .setAlluxioOnly(false)
            .setUnchecked(Configuration.getBoolean(PropertyKey.USER_FILE_DELETE_UNCHECKED))
            .build();
  }
}
