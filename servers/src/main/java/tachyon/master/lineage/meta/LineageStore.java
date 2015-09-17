/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package tachyon.master.lineage.meta;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.client.file.TachyonFile;
import tachyon.dag.DAG;
import tachyon.job.Job;

/**
 * A store of lineages.
 */
public final class LineageStore {
  private DAG<Lineage> mLineageDAG;

  /** Indices for lineages */
  /** Index of the output files of lineage to lineage */
  private Map<TachyonFile, Lineage> mOutputFileIndex;
  private Map<LineageState, Lineage> mStateIndex;
  private Map<Long, Lineage> mIdIndex;

  public LineageStore() {
    mLineageDAG = new DAG<Lineage>();
    mOutputFileIndex = Maps.newHashMap();
    mStateIndex = Maps.newHashMap();
    mIdIndex = Maps.newHashMap();
  }

  public long addLineage(List<TachyonFile> inputFiles, List<TachyonFile> outputFiles, Job job) {
    Lineage lineage  = new Lineage(inputFiles, outputFiles, job);

    List<Lineage> parentLineages = Lists.newArrayList();
    for(TachyonFile inputFile:inputFiles) {
      if(mOutputFileIndex.containsKey(inputFile)) {
        parentLineages.add(mOutputFileIndex.get(inputFile));
      }
    }

    mLineageDAG.add(lineage, parentLineages);

    // update index
    for (TachyonFile outputFile : outputFiles) {
      mOutputFileIndex.put(outputFile, lineage);
    }
    mStateIndex.put(lineage.getState(), lineage);
    mIdIndex.put(lineage.getId(), lineage);

    return lineage.getId();
  }

  private void updateState(Lineage lineage, LineageState newState) {
    lineage.setState(newState);
    mStateIndex.put(newState, lineage);
  }
}
