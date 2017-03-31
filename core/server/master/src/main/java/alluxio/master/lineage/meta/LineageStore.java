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

package alluxio.master.lineage.meta;

import alluxio.collections.DirectedAcyclicGraph;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.Job;
import alluxio.master.journal.JournalCheckpointStreamable;
import alluxio.master.journal.JournalOutputStream;
import alluxio.proto.journal.Lineage.LineageEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;

/**
 * A store of lineages. This class is thread-safe.
 *
 * TODO(yupeng): relax locking
 */
@ThreadSafe
public final class LineageStore implements JournalCheckpointStreamable {
  private final LineageIdGenerator mLineageIdGenerator;
  private final DirectedAcyclicGraph<Lineage> mLineageDAG;

  // Indices for lineages
  /** Index of the output files of lineage to lineage. */
  private Map<Long, Lineage> mOutputFileIndex;
  private Map<Long, Lineage> mIdIndex;

  /**
   * Constructs the lineage store.
   *
   * @param lineageIdGenerator the lineage id generator
   */
  public LineageStore(LineageIdGenerator lineageIdGenerator) {
    mLineageIdGenerator = lineageIdGenerator;
    mLineageDAG = new DirectedAcyclicGraph<>();
    mOutputFileIndex = new HashMap<>();
    mIdIndex = new HashMap<>();
  }

  /**
   * Constructs the lineage store from the journal.
   *
   * @param entry the journal entry
   */
  public synchronized void addLineageFromJournal(LineageEntry entry) {
    Lineage lineage = Lineage.fromJournalEntry(entry);
    createLineageInternal(lineage);
  }

  /**
   * Creates a lineage.
   *
   * @param inputFiles the input files
   * @param outputFiles the output files
   * @param job the job
   * @return the id of the created lineage
   */
  public synchronized long createLineage(List<Long> inputFiles,
      List<Long> outputFiles, Job job) {
    long lineageId = mLineageIdGenerator.generateId();
    Lineage lineage = new Lineage(lineageId, inputFiles, outputFiles, job);
    createLineageInternal(lineage);
    return lineageId;
  }

  private void createLineageInternal(Lineage lineage) {
    List<Lineage> parentLineages = new ArrayList<>();
    for (long inputFile : lineage.getInputFiles()) {
      if (mOutputFileIndex.containsKey(inputFile)) {
        parentLineages.add(mOutputFileIndex.get(inputFile));
      }
    }
    mLineageDAG.add(lineage, parentLineages);

    // update index
    for (long outputFile : lineage.getOutputFiles()) {
      mOutputFileIndex.put(outputFile, lineage);
    }
    mIdIndex.put(lineage.getId(), lineage);
  }

  /**
   * Deletes a lineage.
   *
   * @param lineageId the lineage id
   * @throws LineageDoesNotExistException if the lineage does not exist
   */
  public synchronized void deleteLineage(long lineageId) throws LineageDoesNotExistException {
    LineageDoesNotExistException.check(mIdIndex.containsKey(lineageId),
        ExceptionMessage.LINEAGE_DOES_NOT_EXIST, lineageId);

    Lineage toDelete = mIdIndex.get(lineageId);
    // delete children first
    for (Lineage childLineage : mLineageDAG.getChildren(toDelete)) {
      deleteLineage(childLineage.getId());
    }

    // delete the given node
    mLineageDAG.deleteLeaf(toDelete);
    mIdIndex.remove(lineageId);
    for (long outputFile : toDelete.getOutputFiles()) {
      mOutputFileIndex.remove(outputFile);
    }
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
   * Gets all the children of a given lineage.
   *
   * @param lineage the lineage
   * @return the lineage's children
   * @throws LineageDoesNotExistException if the lineage does not exist
   */
  public synchronized List<Lineage> getChildren(Lineage lineage)
      throws LineageDoesNotExistException {
    LineageDoesNotExistException.check(mIdIndex.containsKey(lineage.getId()),
        ExceptionMessage.LINEAGE_DOES_NOT_EXIST, lineage.getId());

    return mLineageDAG.getChildren(lineage);
  }

  /**
   * Gets the lineage that has the given output file.
   *
   * @param fileId the file id
   * @return the lineage containing the output file
   * @throws LineageDoesNotExistException if the lineage does not exist
   */
  public synchronized Lineage getLineageOfOutputFile(long fileId)
      throws LineageDoesNotExistException {
    Lineage lineage = mOutputFileIndex.get(fileId);
    LineageDoesNotExistException.check(lineage != null, ExceptionMessage.LINEAGE_DOES_NOT_EXIST,
        fileId);
    return lineage;
  }

  /**
   * Gets all the parents of a given lineage.
   *
   * @param lineage the lineage
   * @return the lineage's parents
   * @throws LineageDoesNotExistException if the lineage does not exist
   */
  public synchronized List<Lineage> getParents(Lineage lineage)
      throws LineageDoesNotExistException {
    LineageDoesNotExistException.check(mIdIndex.containsKey(lineage.getId()),
        ExceptionMessage.LINEAGE_DOES_NOT_EXIST, lineage.getId());

    return mLineageDAG.getParents(lineage);
  }

  /**
   * @return the list of all root lineages
   */
  public synchronized List<Lineage> getRootLineages() {
    return mLineageDAG.getRoots();
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
      outputStream.write(lineage.toJournalEntry());
    }
  }

  /**
   * Checks if there's an output file with given file id.
   *
   * @param fileId the file id
   * @return true if there's a lineage in the store that has the output file of the given id, false
   *         otherwise
   */
  public synchronized boolean hasOutputFile(long fileId) {
    return mOutputFileIndex.containsKey(fileId);
  }
}
