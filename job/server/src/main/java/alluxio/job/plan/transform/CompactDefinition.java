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
import alluxio.job.plan.transform.compact.Compactor;
import alluxio.job.plan.transform.compact.SequentialCompactor;
import alluxio.job.plan.transform.format.TableReader;
import alluxio.job.plan.transform.format.TableSchema;
import alluxio.job.plan.transform.format.TableWriter;
import alluxio.job.util.SerializableVoid;
import alluxio.util.CommonUtils;
import alluxio.wire.WorkerInfo;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
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
    for (URIStatus status : context.getFileSystem().listStatus(inputDir)) {
      if (!shouldIgnore(status)) {
        files.add(status);
      }
    }
    Map<WorkerInfo, ArrayList<CompactTask>> assignments = Maps.newHashMap();
    int groupSize = Math.max(1, (files.size() + 1) / config.getNumFiles());
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
    Closer closer = Closer.create();
    boolean closed = false;
    Compactor compactor = new SequentialCompactor();
    for (CompactTask task : tasks) {
      ArrayList<String> inputs = task.getInputs();
      if (inputs.isEmpty()) {
        continue;
      }
      AlluxioURI output = new AlluxioURI(task.getOutput());
      List<TableReader> readers = Lists.newArrayList();
      TableWriter writer = null;
      try {
        for (String input : inputs) {
          readers.add(closer.register(TableReader.create(new AlluxioURI(input),
              config.getPartitionInfo())));
        }
        TableSchema schema = readers.get(0).getSchema();
        writer = closer.register(TableWriter.create(schema, output));
        compactor.compact(readers, writer);
      } catch (Throwable t) {
        closer.close();
        closed = true;
        try {
          context.getFileSystem().delete(output); // output is the compacted file
        } catch (Throwable e) {
          t.addSuppressed(e);
        }
        closer.rethrow(t);
      } finally {
        if (!closed) {
          closer.close();
        }
      }
    }
    return null;
  }
}
