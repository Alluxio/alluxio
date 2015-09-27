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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import tachyon.client.file.TachyonFile;
import tachyon.dag.DAG;
import tachyon.job.Job;
import tachyon.master.journal.JournalCheckpointStreamable;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.lineage.journal.LineageEntry;
import tachyon.thrift.LineageDoesNotExistException;

/**
 * A store of lineages. This class is thread-safe.
 *
 * TODO(yupeng): relax locking
 */
public final class LineageStore implements JournalCheckpointStreamable {
  private final LineageIdGenerator mLineageIdGenerator;
  private final DAG<Lineage> mLineageDAG;

  /** Indices for lineages */
  /** Index of the output files of lineage to lineage */
  private Map<Long, Lineage> mOutputFileIndex;
  private Map<Long, Lineage> mIdIndex;

  public LineageStore(LineageIdGenerator lineageIdGenerator) {
    mLineageIdGenerator = lineageIdGenerator;
    mLineageDAG = new DAG<Lineage>();
    mOutputFileIndex = Maps.newHashMap();
    mIdIndex = Maps.newHashMap();
  }

  public synchronized void addLineageFromJournal(LineageEntry entry) {
    Lineage lineage = entry.toLineage();
    addLineageInternal(lineage);
  }

  public synchronized long createLineage(List<TachyonFile> inputFiles,
      List<LineageFile> outputFiles, Job job) {
    long lineageId = mLineageIdGenerator.generateId();
    Lineage lineage = new Lineage(lineageId, inputFiles, outputFiles, job);
    addLineageInternal(lineage);
    return lineageId;
  }

  private void addLineageInternal(Lineage lineage) {
    List<Lineage> parentLineages = Lists.newArrayList();
    for (TachyonFile inputFile : lineage.getInputFiles()) {
      if (mOutputFileIndex.containsKey(inputFile.getFileId())) {
        parentLineages.add(mOutputFileIndex.get(inputFile.getFileId()));
      }
    }
    mLineageDAG.add(lineage, parentLineages);

    // update index
    for (TachyonFile outputFile : lineage.getOutputFiles()) {
      mOutputFileIndex.put(outputFile.getFileId(), lineage);
    }
    mIdIndex.put(lineage.getId(), lineage);
  }

  public synchronized void completeFile(long fileId) {
    Preconditions.checkState(mOutputFileIndex.containsKey(fileId));
    Lineage lineage = mOutputFileIndex.get(fileId);
    lineage.updateOutputFileState(fileId, LineageFileState.COMPLETED);
  }

  public synchronized void deleteLineage(long lineageId) {
    Preconditions.checkState(mIdIndex.containsKey(lineageId),
        "lineage id " + lineageId + " does not exist");
    Lineage toDelete = mIdIndex.get(lineageId);

    // delete children first
    for (Lineage childLineage : mLineageDAG.getChildren(toDelete)) {
      deleteLineage(childLineage.getId());
    }

    // delete the given node
    mLineageDAG.deleteLeaf(toDelete);
    mIdIndex.remove(lineageId);
    for (TachyonFile outputFile : toDelete.getOutputFiles()) {
      mOutputFileIndex.remove(outputFile.getFileId());
    }
  }

  public synchronized void requestFilePersistence(long fileId) {
    Preconditions.checkState(mOutputFileIndex.containsKey(fileId));
    Lineage lineage = mOutputFileIndex.get(fileId);
    lineage.updateOutputFileState(fileId, LineageFileState.PERSISENCE_REQUESTED);
  }

  public synchronized Lineage getLineage(long lineageId) {
    return mIdIndex.get(lineageId);
  }

  public synchronized List<Lineage> getChildren(Lineage lineage) {
    Preconditions.checkState(mIdIndex.containsKey(lineage.getId()),
        "lineage id " + lineage.getId() + " does not exist");

    return mLineageDAG.getChildren(lineage);
  }

  public synchronized List<Lineage> getParents(Lineage lineage) {
    Preconditions.checkState(mIdIndex.containsKey(lineage.getId()),
        "lineage id " + lineage.getId() + " does not exist");

    return mLineageDAG.getParents(lineage);
  }

  public synchronized Lineage reportLostFile(long fileId) {
    Lineage lineage = mOutputFileIndex.get(fileId);
    // TODO(yupeng) push the persisted info to FS master
    if (lineage.getOutputFileState(fileId) != LineageFileState.PERSISTED) {
      lineage.updateOutputFileState(fileId, LineageFileState.LOST);
    }
    return lineage;
  }

  /**
   * Gets all the root lineages.
   */
  public synchronized List<Lineage> getRootLineages() {
    return mLineageDAG.getRoots();
  }

  public synchronized void commitFilePersistence(Long fileId) {
    Lineage lineage = mOutputFileIndex.get(fileId);
    lineage.updateOutputFileState(fileId, LineageFileState.PERSISTED);
  }

  public synchronized List<Lineage> sortLineageTopologically(Set<Lineage> lineages) {
    return mLineageDAG.sortTopologically(lineages);
  }

  public synchronized List<Lineage> getAllInTopologicalOrder() {
    return mLineageDAG.getAllInTopologicalOrder();
  }

  @Override
  public synchronized void streamToJournalCheckpoint(JournalOutputStream outputStream)
      throws IOException {
    // write the lineages out in a topological order
    for (Lineage lineage : mLineageDAG.getAllInTopologicalOrder()) {
      outputStream.writeEntry(lineage.toJournalEntry());
    }
  }

  /**
   * @param fileId the fild id
   * @return the lineage state of the given file
   * @throws LineageDoesNotExistException
   */
  public synchronized LineageFileState getLineageFileState(long fileId)
      throws LineageDoesNotExistException {
    if (!mOutputFileIndex.containsKey(fileId)) {
      throw new LineageDoesNotExistException("No lineage has output file " + fileId);
    }
    return mOutputFileIndex.get(fileId).getOutputFileState(fileId);
  }
}
