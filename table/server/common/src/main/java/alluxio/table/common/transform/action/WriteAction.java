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
import alluxio.job.JobConfig;
import alluxio.job.plan.transform.CompactConfig;
import alluxio.table.common.Layout;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;

import java.util.List;
import java.util.Map;

/**
 * The definition of the write action.
 */
public class WriteAction implements TransformAction {
  private static final String NAME = "write";
  private static final String NUM_FILES_OPTION = "hive.file.count.max";
  private static final String FILE_SIZE_OPTION = "hive.file.size.min";
  private static final long DEFAULT_FILE_SIZE = FileUtils.ONE_GB * 2;
  private static final int DEFAULT_NUM_FILES = 100;

  /**
   * Layout type, for example "hive".
   */
  private final String mLayoutType;
  /**
   * Expected number of files after compaction.
   */
  private final int mNumFiles;
  /**
   * Default file size after coalescing.
   */
  private final long mFileSize;

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
      long fileSize = options.containsKey(FILE_SIZE_OPTION)
          ? Long.parseLong(options.get(FILE_SIZE_OPTION))
          : DEFAULT_FILE_SIZE;
      Preconditions.checkArgument(numFiles >= 0,
          ExceptionMessage.TRANSFORM_WRITE_ACTION_INVALID_NUM_FILES);
      return new WriteAction(type, numFiles, fileSize);
    }
  }

  private WriteAction(String type, int numFiles, long fileSize) {
    mLayoutType = type;
    mNumFiles = numFiles;
    mFileSize = fileSize;
  }

  @Override
  public JobConfig generateJobConfig(Layout base, Layout transformed) {
    alluxio.job.plan.transform.PartitionInfo basePartitionInfo =
        TransformActionUtils.generatePartitionInfo(base);
    return new CompactConfig(basePartitionInfo, base.getLocation().toString(),
        transformed.getLocation().toString(),
        mLayoutType, mNumFiles, mFileSize);
  }
}
