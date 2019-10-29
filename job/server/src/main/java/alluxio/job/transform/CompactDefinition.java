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

package alluxio.job.transform;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.job.AbstractVoidJobDefinition;
import alluxio.job.RunTaskContext;
import alluxio.job.SelectExecutorsContext;
import alluxio.job.transform.compact.Compactor;
import alluxio.job.transform.compact.SequentialCompactor;
import alluxio.job.transform.format.TableReader;
import alluxio.job.transform.format.TableSchema;
import alluxio.job.transform.format.TableWriter;
import alluxio.job.util.SerializableVoid;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The job definition for compacting files representing a structured table under a directory.
 */
public final class CompactDefinition
    extends AbstractVoidJobDefinition<CompactConfig, ArrayList<CompactTask>> {
  private static final Logger LOG = LoggerFactory.getLogger(CompactDefinition.class);
  private static final String COMPACTED_FILE_PATTERN = "part-%d.parquet";

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

  @Override
  public Map<WorkerInfo, ArrayList<CompactTask>> selectExecutors(CompactConfig config,
      List<WorkerInfo> jobWorkers, SelectExecutorsContext context) throws Exception {
    Preconditions.checkState(!jobWorkers.isEmpty(), "No job worker");
    AlluxioURI inputDir = new AlluxioURI(config.getInput());
    AlluxioURI outputDir = new AlluxioURI(config.getOutput());

    List<URIStatus> files = Lists.newArrayList();
    for (URIStatus status : context.getFileSystem().listStatus(inputDir)) {
      if (!status.isFolder()) {
        files.add(status);
      }
    }
    Map<WorkerInfo, ArrayList<CompactTask>> assignments = Maps.newHashMap();
    int groupSize = (files.size() + 1) / config.getNumFiles();
    if  (groupSize == 0) {
      // Number of existing files is less than the expected number of files after compaction,
      // so there is no need to compact any files.
      return assignments;
    }
    // Files to be compacted are grouped into different groups,
    // each group of files are compacted to one file,
    // one task is to compact one group of files,
    // different tasks are assigned to different workers in a round robin way.
    ArrayList<String> group = new ArrayList<>(groupSize);
    int workerIndex = 0;
    int outputIndex = 0;
    for (int i = 0; i < files.size(); i++) {
      URIStatus file = files.get(i);
      group.add(inputDir.join(file.getName()).toString());
      if (group.size() == groupSize || i == files.size() - 1) {
        WorkerInfo worker = jobWorkers.get(workerIndex++);
        if (workerIndex == jobWorkers.size()) {
          workerIndex = 0;
        }
        if (!assignments.containsKey(worker)) {
          assignments.put(worker, new ArrayList<>());
        }
        ArrayList<CompactTask> tasks = assignments.get(worker);
        tasks.add(new CompactTask(group, getOutputPath(outputDir, outputIndex++)));
        group = new ArrayList<>(groupSize);
      }
    }
    return assignments;
  }

  @Override
  public SerializableVoid runTask(CompactConfig config, ArrayList<CompactTask> tasks,
      RunTaskContext context) throws Exception {
    Compactor compactor = new SequentialCompactor();
    for (CompactTask task : tasks) {
      ArrayList<String> inputs = task.getInputs();
      if (inputs.isEmpty()) {
        continue;
      }
      String output = task.getOutput();
      List<TableReader> readers = Lists.newArrayList();
      TableWriter writer = null;
      try {
        for (String input : inputs) {
          readers.add(TableReader.create(new AlluxioURI(input), config.getPartitionInfo()));
        }
        TableSchema schema = readers.get(0).getSchema();
        writer = TableWriter.create(schema, new AlluxioURI(output));
        compactor.compact(readers, writer);
      } finally {
        for (TableReader reader : readers) {
          reader.close();
        }
        if (writer != null) {
          writer.close();
        }
      }
    }
    return null;
  }
}
