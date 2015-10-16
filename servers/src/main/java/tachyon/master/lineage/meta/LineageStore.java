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
import com.google.common.collect.Sets;

import tachyon.client.file.TachyonFile;
import tachyon.collections.DirectedAcyclicGraph;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.job.Job;
import tachyon.master.journal.JournalCheckpointStreamable;
import tachyon.master.journal.JournalOutputStream;
import tachyon.master.lineage.journal.LineageEntry;

/**
 * A store of lineages. This class is thread-safe.
 *
 * TODO(yupeng): relax locking
 */
public final class LineageStore implements JournalCheckpointStreamable {
  private final LineageIdGenerator mLineageIdGenerator;
  private final DirectedAcyclicGraph<Lineage> mLineageDAG;

  /** Indices for lineages */
  /** Index of the output files of lineage to lineage */
  private Map<Long, Lineage> mOutputFileIndex;
  private Map<Long, Lineage> mIdIndex;

  /**
   * Constructs the lineage store.
   *
   * @param lineageIdGenerator the lineage id generator
   */
  public LineageStore(LineageIdGenerator lineageIdGenerator) {
    mLineageIdGenerator = lineageIdGenerator;
    mLineageDAG = new DirectedAcyclicGraph<Lineage>();
    mOutputFileIndex = Maps.newHashMap();
    mIdIndex = Maps.newHashMap();
  }

  /**
   * Constructs the lineage store from the journal.
   *
   * @param entry the journal entry
   */
  public synchronized void addLineageFromJournal(LineageEntry entry) {
    Lineage lineage = entry.toLineage();
    addLineageInternal(lineage);
  }

  /**
   * Creates a lineage.
   *
   * @param inputFiles the input files
   * @param outputFiles the output files
   * @param job the job
   * @return the id of the created lineage
   */
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

  /**
   * Completes an output file.
   *
   * @param fileId the file id
   */
  public synchronized void completeFile(long fileId) {
    Preconditions.checkState(mOutputFileIndex.containsKey(fileId));
    Lineage lineage = mOutputFileIndex.get(fileId);
    lineage.updateOutputFileState(fileId, LineageFileState.COMPLETED);
  }

  /**
   * Deletes a lineage.
   *
   * @param lineageId the lineage id
   */
  public synchronized void deleteLineage(long lineageId) {
    Preconditions.checkState(mIdIndex.containsKey(lineageId),
        "lineage id " + lineageId + " does not exist");

    deleteLineage(lineageId, Sets.<Long>newHashSet());
  }

  private void deleteLineage(long lineageId, Set<Long> deleted) {
    if (deleted.contains(lineageId)) {
      return;
    }

    Lineage toDelete = mIdIndex.get(lineageId);
    // delete children first
    for (Lineage childLineage : mLineageDAG.getChildren(toDelete)) {
      deleteLineage(childLineage.getId());
    }

    // delete the given node
    mLineageDAG.deleteLeaf(toDelete);
    mIdIndex.remove(lineageId);
    deleted.add(lineageId);
    for (TachyonFile outputFile : toDelete.getOutputFiles()) {
      mOutputFileIndex.remove(outputFile.getFileId());
    }
  }

  /**
   * Requests an output file as being persisted.
   *
   * @param fileId the file id
   */
  public synchronized void requestFilePersistence(long fileId) {
    Preconditions.checkState(mOutputFileIndex.containsKey(fileId));
    Lineage lineage = mOutputFileIndex.get(fileId);
    lineage.updateOutputFileState(fileId, LineageFileState.PERSISENCE_REQUESTED);
  }

  /**
   * Gets the lineage.
   *
   * @param lineageId the lineage id
   * @return the lineage
   */
  public synchronized Lineage getLineage(long lineageId) {
    return mIdIndex.get(lineageId);
  }

  /**
   * Gets all the children of a given lineage
   *
   * @param lineage the lineage
   * @return the lineage's children
   */
  public synchronized List<Lineage> getChildren(Lineage lineage) {
    Preconditions.checkState(mIdIndex.containsKey(lineage.getId()),
        "lineage id " + lineage.getId() + " does not exist");

    return mLineageDAG.getChildren(lineage);
  }

  /**
   * Gets all the parents of a given lineage
   *
   * @param lineage the lineage
   * @return the lineage's parents
   */
  public synchronized List<Lineage> getParents(Lineage lineage) {
    Preconditions.checkState(mIdIndex.containsKey(lineage.getId()),
        "lineage id " + lineage.getId() + " does not exist");

    return mLineageDAG.getParents(lineage);
  }

  /**
   * Reports an output file as lost.
   *
   * @param fileId the file id
   * @return the lineage containing the output file, null if no lineage outputs the given file
   */
  public synchronized Lineage reportLostFile(long fileId) {
    Lineage lineage = mOutputFileIndex.get(fileId);
    if (lineage == null) {
      return null;
    }
    // TODO(yupeng) push the persisted info to FS master
    if (lineage.getOutputFileState(fileId) != LineageFileState.PERSISTED) {
      lineage.updateOutputFileState(fileId, LineageFileState.LOST);
    }
    return lineage;
  }

  /**
   * @return the list of all root lineages
   */
  public synchronized List<Lineage> getRootLineages() {
    return mLineageDAG.getRoots();
  }

  /**
   * Commits a file as persisted
   *
   * @param fileId the file id
   */
  public synchronized void commitFilePersistence(Long fileId) {
    Preconditions.checkState(mOutputFileIndex.containsKey(fileId),
        "file id " + fileId + " does not belong to any lineage");

    Lineage lineage = mOutputFileIndex.get(fileId);
    lineage.updateOutputFileState(fileId, LineageFileState.PERSISTED);
  }

  /**
   * Sorts a given set of lineages topologically.
   *
   * @param lineages lineages to sort
   * @return the lineages after sort
   */
  public synchronized List<Lineage> sortLineageTopologically(Set<Lineage> lineages) {
    return mLineageDAG.sortTopologically(lineages);
  }

  /**
   * @return all the lineages in topological order
   */
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
   * Checks if there's an output file with given file id
   *
   * @param fileId the file id
   * @return true if there's a lineage in the store that has the output file of the given id, false
   *         otherwise
   */
  public boolean hasOutputFile(long fileId) {
    return mOutputFileIndex.containsKey(fileId);
  }

  /**
   * @param fileId the fild id
   * @return the lineage state of the given file
   * @throws LineageDoesNotExistException if the file does not belong to any lineage
   */
  public synchronized LineageFileState getLineageFileState(long fileId)
      throws LineageDoesNotExistException {
    if (!mOutputFileIndex.containsKey(fileId)) {
      throw new LineageDoesNotExistException("No lineage has output file " + fileId);
    }
    return mOutputFileIndex.get(fileId).getOutputFileState(fileId);
  }
}
