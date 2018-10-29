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
package alluxio.client.file;

import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.util.grpc.GrpcUtils;
import alluxio.wire.LoadMetadataType;

public final class FileSystemClientOptions {
    private static FileSystemMasterCommonPOptions mCommonOptions;
    private static GetStatusPOptions mGetStatusOptions;
    static {
        RefreshDefaultValues();
    }
    public static void RefreshDefaultValues() {
        // Defaults for common options
        FileSystemMasterCommonPOptions.Builder commonOptionsBuilder =
                FileSystemMasterCommonPOptions.newBuilder();
        commonOptionsBuilder.setTtl(Constants.NO_TTL);
        commonOptionsBuilder.setTtlAction(alluxio.grpc.TtlAction.DELETE);
        commonOptionsBuilder.setSyncIntervalMs(
                Configuration.getMs(
                        PropertyKey.USER_FILE_METADATA_SYNC_INTERVAL));
        mCommonOptions = commonOptionsBuilder.build();

        // Defaults for GetStatus option classes
        GetStatusPOptions.Builder getStatusOptionsBuilder =
                GetStatusPOptions.newBuilder();
        getStatusOptionsBuilder.setCommonOptions(mCommonOptions);
        getStatusOptionsBuilder.setLoadMetadataType(
                GrpcUtils.toProto( Configuration.getEnum(
                        PropertyKey.USER_FILE_METADATA_LOAD_TYPE, LoadMetadataType.class )));
        mGetStatusOptions = getStatusOptionsBuilder.build();
    }
    public static FileSystemMasterCommonPOptions getCommonOptions() {
        return FileSystemMasterCommonPOptions.newBuilder(mCommonOptions).build();
    }
    public static GetStatusPOptions getGetStatusOptions() {
        return GetStatusPOptions.newBuilder(mGetStatusOptions).build();
    }
}