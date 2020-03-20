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

package alluxio.table.common.layout;

import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.table.common.Layout;
import alluxio.table.common.LayoutFactory;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;

/**
 * Glue layout factory to create layout implementation.
 */
public class GlueLayoutFactory implements LayoutFactory {
  public static final String TYPE = "hive";

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public Layout create(alluxio.grpc.table.Layout layoutProto) {
    if (!TYPE.equals(layoutProto.getLayoutType())) {
      throw new IllegalStateException(
          "Cannot parse GlueLayout from layout type: " + layoutProto.getLayoutType());
    }
    if (!layoutProto.hasLayoutData()) {
      throw new IllegalStateException("Cannot parse layout from empty layout data");
    }

    try {
      PartitionInfo partitionInfo = PartitionInfo.parseFrom(layoutProto.getLayoutData());
      return new GlueLayout(partitionInfo, new ArrayList<>(layoutProto.getStatsMap().values()));
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalStateException("Cannot parse GlueLayout from proto layout", e);
    }
  }
}
