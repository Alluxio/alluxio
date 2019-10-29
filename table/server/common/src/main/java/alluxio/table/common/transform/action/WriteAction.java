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

import alluxio.exception.ExceptionMessage;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.job.JobConfig;
import alluxio.job.transform.CompactConfig;
import alluxio.job.transform.SchemaField;
import alluxio.table.ProtoUtils;
import alluxio.table.common.Layout;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The definition of the write action.
 */
public class WriteAction implements TransformAction {
  private static final String NAME = "write";
  private static final String NUM_FILES_OPTION = "hive.num.files";
  private static final int DEFAULT_NUM_FILES = 1;

  /**
   * Layout type, for example "hive".
   */
  private final String mLayoutType;
  /**
   * Expected number of files after compaction.
   */
  private final int mNumFiles;

  /**
   * Factory to create an instance.
   */
  public static class WriteActionFactory implements TransformActionFactory {
    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public TransformAction create(String definition, List<String> args,
        Map<String, String> options) {
      Preconditions.checkArgument(args.size() == 1,
          ExceptionMessage.TRANSFORM_WRITE_ACTION_INVALID_ARGS.toString());
      String type = args.get(0);
      int numFiles = options.containsKey(NUM_FILES_OPTION)
          ? Integer.parseInt(options.get(NUM_FILES_OPTION))
          : DEFAULT_NUM_FILES;
      Preconditions.checkArgument(numFiles > 0,
          ExceptionMessage.TRANSFORM_WRITE_ACTION_INVALID_NUM_FILES);
      return new WriteAction(type, numFiles);
    }
  }

  private WriteAction(String type, int numFiles) {
    mLayoutType = type;
    mNumFiles = numFiles;
  }

  @Override
  public JobConfig generateJobConfig(Layout base, Layout transformed) {
    PartitionInfo partitionInfo;
    try {
      partitionInfo = ProtoUtils.toHiveLayout(base.toProto());
    } catch (InvalidProtocolBufferException e) {
      return new CompactConfig(null, base.getLocation().toString(),
          transformed.getLocation().toString(),
          mLayoutType, mNumFiles);
    }
    String serdeClass = partitionInfo.getStorage().getStorageFormat().getSerde();
    String inputFormat = partitionInfo.getStorage().getStorageFormat().getInputFormat();

    ArrayList<SchemaField> colList = new ArrayList<>(partitionInfo.getDataColsList().size());
    for (FieldSchema col : partitionInfo.getDataColsList()) {
      colList.add(new SchemaField(col.getId(), col.getName(), col.getType(),
          col.getOptional(), col.getComment()));
    }
    alluxio.job.transform.PartitionInfo transformPartInfo
        = new alluxio.job.transform.PartitionInfo(serdeClass, inputFormat,
        new HashMap<>(partitionInfo.getStorage().getSerdeParametersMap()), colList);
    return new CompactConfig(transformPartInfo, base.getLocation().toString(),
        transformed.getLocation().toString(),
        mLayoutType, mNumFiles);
  }
}
