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

package alluxio.job.plan.transform;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.collections.Pair;
import alluxio.job.plan.AbstractVoidPlanDefinition;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.plan.transform.format.TableReader;
import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.TableSchema;
import alluxio.job.plan.transform.format.TableWriter;
import alluxio.job.util.SerializableVoid;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The job definition for compacting files representing a structured table under a directory.
 */
public final class CompactDefinition
    extends AbstractVoidPlanDefinition<CompactConfig, ArrayList<CompactTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactDefinition.class);
  private static final int TASKS_PER_WORKER = 10;
  private static final String COMPACTED_FILE_PATTERN = "part-%d.parquet";
  private static final String SUCCESS_FILENAME = "_SUCCESS";
  private static final String CRC_FILENAME_SUFFIX = ".crc";

  private static final Map<Format, Double> COMPRESSION_RATIO = ImmutableMap.of(
      Format.PARQUET, 1.0,
      Format.CSV, 5.0,
      Format.GZIP_CSV, 2.5,
      Format.ORC, 1.0);

  /**
   * Constructs a new {@link CompactDefinition}.
   */
  public CompactDefinition() {
  }

  @Override
  public Class<CompactConfig> getJobConfigClass() {
    return CompactConfig.class;
  }

  private String getOutputPath(AlluxioURI outputDir, int outputIndex) {
    return outputDir.join(String.format(COMPACTED_FILE_PATTERN, outputIndex))
        .toString();
  }

  private boolean shouldIgnore(URIStatus status) {
    return status.isFolder()
        || status.getName().equals(SUCCESS_FILENAME)
        || status.getName().endsWith(CRC_FILENAME_SUFFIX);
  }

  @Override
  public Set<Pair<WorkerInfo, ArrayList<CompactTask>>> selectExecutors(CompactConfig config,
      List<WorkerInfo> jobWorkers, SelectExecutorsContext context) throws Exception {
    Preconditions.checkState(!jobWorkers.isEmpty(), "No job worker");
    AlluxioURI inputDir = new AlluxioURI(config.getInput());
    AlluxioURI outputDir = new AlluxioURI(config.getOutput());

    List<URIStatus> files = Lists.newArrayList();
    // use double to prevent overflow
    double totalFileSize = 0;
    for (URIStatus status : context.getFileSystem().listStatus(inputDir)) {
      if (!shouldIgnore(status)) {
        files.add(status);
        totalFileSize += status.getLength();
      }
    }

    Map<WorkerInfo, ArrayList<CompactTask>> assignments = Maps.newHashMap();
    int maxNumFiles = config.getMaxNumFiles();
    long groupMinSize = config.getMinFileSize();

    if (!files.isEmpty() && config.getInputPartitionInfo() != null) {
      // adjust the group minimum size for source compression ratio
      groupMinSize *= COMPRESSION_RATIO.get(
          config.getInputPartitionInfo().getFormat(files.get(0).getName()));
    }

    if (totalFileSize / groupMinSize > maxNumFiles) {
      groupMinSize = Math.round(totalFileSize / maxNumFiles);
    }

    // Files to be compacted are grouped into different groups,
    // each group of files are compacted to one file,
    // one task is to compact one group of files,
    // different tasks are assigned to different workers in a round robin way.
    // We keep adding files to the group, until adding more files makes it too big.
    ArrayList<String> group = new ArrayList<>();
    int workerIndex = 0;
    int outputIndex = 0;
    // Number of groups already generated
    int groupIndex = 0;
    long currentGroupSize = 0;
    long halfGroupMinSize = groupMinSize / 2;
    for (URIStatus file : files) {
      // add the file to the group if
      // 1. group is empty
      // 2. group is the last group
      // 3. group size with the new file is closer to the groupMinSize than group size without it
      if (group.isEmpty() || groupIndex == maxNumFiles - 1
          || (currentGroupSize + file.getLength()) <= halfGroupMinSize
          || (Math.abs(currentGroupSize + file.getLength() - groupMinSize)
          <= Math.abs(currentGroupSize - groupMinSize))) {
        group.add(inputDir.join(file.getName()).toString());
        currentGroupSize += file.getLength();
      } else {
        WorkerInfo worker = jobWorkers.get(workerIndex++);
        if (workerIndex == jobWorkers.size()) {
          workerIndex = 0;
        }
        if (!assignments.containsKey(worker)) {
          assignments.put(worker, new ArrayList<>());
        }
        ArrayList<CompactTask> tasks = assignments.get(worker);
        tasks.add(new CompactTask(group, getOutputPath(outputDir, outputIndex++)));
        group = new ArrayList<>();
        group.add(inputDir.join(file.getName()).toString());
        currentGroupSize = file.getLength();
        groupIndex++;
      }
    }
    // handle the last group
    if (!group.isEmpty()) {
      WorkerInfo worker = jobWorkers.get(workerIndex);
      if (!assignments.containsKey(worker)) {
        assignments.put(worker, new ArrayList<>());
      }
      ArrayList<CompactTask> tasks = assignments.get(worker);
      tasks.add(new CompactTask(group, getOutputPath(outputDir, outputIndex)));
    }

    Set<Pair<WorkerInfo, ArrayList<CompactTask>>> result = Sets.newHashSet();
    for (Map.Entry<WorkerInfo, ArrayList<CompactTask>> assignment : assignments.entrySet()) {
      List<List<CompactTask>> partitioned = CommonUtils.partition(
          assignment.getValue(), TASKS_PER_WORKER);
      for (List<CompactTask> compactTasks : partitioned) {
        if (!compactTasks.isEmpty()) {
          result.add(new Pair<>(assignment.getKey(), Lists.newArrayList(compactTasks)));
        }
      }
    }

    return result;
  }

  @Override
  public SerializableVoid runTask(CompactConfig config, ArrayList<CompactTask> tasks,
      RunTaskContext context) throws Exception {
    for (CompactTask task : tasks) {
      ArrayList<String> inputs = task.getInputs();
      if (inputs.isEmpty()) {
        continue;
      }
      AlluxioURI output = new AlluxioURI(task.getOutput());

      TableSchema schema;
      try (TableReader reader = TableReader.create(new AlluxioURI(inputs.get(0)),
          config.getInputPartitionInfo())) {
        schema = reader.getSchema();
      }

      try (TableWriter writer = TableWriter.create(schema, output,
          config.getOutputPartitionInfo())) {
        for (String input : inputs) {
          try (TableReader reader = TableReader.create(new AlluxioURI(input),
              config.getInputPartitionInfo())) {
            for (TableRow row = reader.read(); row != null; row = reader.read()) {
              writer.write(row);
            }
          }
        }
      } catch (Throwable e) {
        try {
          context.getFileSystem().delete(output); // outputUri is the output file
        } catch (Throwable t) {
          e.addSuppressed(t);
        }
        throw e;
      }
    }
    return null;
  }
}
