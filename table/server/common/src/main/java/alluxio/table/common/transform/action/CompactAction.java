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
import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * The definition of the compact action.
 */
public class CompactAction implements TransformAction {

  private static final String NUM_FILES_OPTION = "file.count.max";
  private static final String FILE_SIZE_OPTION = "file.size.min";
  private static final long DEFAULT_FILE_SIZE = FileUtils.ONE_GB * 2;
  private static final int DEFAULT_NUM_FILES = 100;

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
  public static class CompactActionFactory implements TransformActionFactory {

    @Override
    public int getOrder() {
      return 0;
    }

    @Override
    public TransformAction create(Properties properties) {
      final String numFilesString = properties.getProperty(NUM_FILES_OPTION);

      final String fileSizeString = properties.getProperty(FILE_SIZE_OPTION);

      if (StringUtils.isEmpty(numFilesString) && StringUtils.isEmpty(fileSizeString)) {
        return null;
      }

      int numFiles = DEFAULT_NUM_FILES;
      if (!StringUtils.isEmpty(numFilesString)) {
        numFiles = Integer.parseInt(numFilesString);
      }

      long fileSize = DEFAULT_FILE_SIZE;
      if (!StringUtils.isEmpty(fileSizeString)) {
        fileSize = Long.parseLong(fileSizeString);
      }

      Preconditions.checkArgument(numFiles > 0,
          ExceptionMessage.TRANSFORM_WRITE_ACTION_INVALID_NUM_FILES.getMessage());
      return new CompactAction(numFiles, fileSize);
    }

    @Override
    public String toString() {
      return "CompactActionFactory";
    }
  }

  private CompactAction(int numFiles, long fileSize) {
    mNumFiles = numFiles;
    mFileSize = fileSize;
  }

  @Override
  public JobConfig generateJobConfig(Layout base, Layout transformed, boolean deleteSrc) {
    alluxio.job.plan.transform.PartitionInfo basePartitionInfo =
        TransformActionUtils.generatePartitionInfo(base);
    alluxio.job.plan.transform.PartitionInfo transformedPartitionInfo =
        TransformActionUtils.generatePartitionInfo(transformed);
    return new CompactConfig(basePartitionInfo, base.getLocation().toString(),
        transformedPartitionInfo, transformed.getLocation().toString(), mNumFiles, mFileSize);
  }
}
