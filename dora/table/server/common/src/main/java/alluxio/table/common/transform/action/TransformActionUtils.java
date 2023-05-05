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

package alluxio.table.common.transform.action;

import alluxio.job.plan.transform.FieldSchema;
import alluxio.job.plan.transform.PartitionInfo;
import alluxio.table.ProtoUtils;
import alluxio.table.common.Layout;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Utilities for implementing {@link TransformAction}.
 */
public class TransformActionUtils {
  private TransformActionUtils() {} // Prevents initialization

  /**
   * @param layout the layout to retrieve partition info from
   * @return the generated partition info
   */
  public static PartitionInfo generatePartitionInfo(Layout layout) {
    alluxio.grpc.table.layout.hive.PartitionInfo partitionInfo;
    try {
      partitionInfo = ProtoUtils.toHiveLayout(layout.toProto());
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException(e);
    }
    String serdeClass = partitionInfo.getStorage().getStorageFormat().getSerde();
    String inputFormat = partitionInfo.getStorage().getStorageFormat().getInputFormat();

    ArrayList<FieldSchema> colList = new ArrayList<>(partitionInfo.getDataColsList().size());
    for (alluxio.grpc.table.FieldSchema col : partitionInfo.getDataColsList()) {
      colList.add(new FieldSchema(col.getId(), col.getName(), col.getType(), col.getComment()));
    }

    return new alluxio.job.plan.transform.PartitionInfo(serdeClass, inputFormat,
        new HashMap<>(partitionInfo.getStorage().getStorageFormat().getSerdelibParametersMap()),
        new HashMap<>(partitionInfo.getParametersMap()),
        colList);
  }
}
