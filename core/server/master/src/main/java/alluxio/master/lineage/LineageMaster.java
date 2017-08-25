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

package alluxio.master.lineage;

import alluxio.AlluxioURI;
import alluxio.exception.AccessControlException;
import alluxio.exception.BlockInfoException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.Job;
import alluxio.master.Master;
import alluxio.master.lineage.checkpoint.CheckpointPlan;
import alluxio.master.lineage.meta.LineageStoreView;
import alluxio.wire.LineageInfo;
import alluxio.wire.TtlAction;

import java.io.IOException;
import java.util.List;

/**
 * The lineage master interface stores the lineage metadata in Alluxio, this interface contains
 * common components that manage all lineage-related activities.
 */
public interface LineageMaster extends Master {
  /**
   * @return a lineage store view wrapping the contained lineage store
   */
  LineageStoreView getLineageStoreView();

  /**
   * Creates a lineage. It creates a new file for each output file.
   *
   * @param inputFiles the input files
   * @param outputFiles the output files
   * @param job the job
   * @return the id of the created lineage
   * @throws InvalidPathException if the path to the input file is invalid
   * @throws FileAlreadyExistsException if the output file already exists
   * @throws BlockInfoException if fails to create the output file
   * @throws AccessControlException if the permission check fails
   * @throws FileDoesNotExistException if any of the input files do not exist
   */
  long createLineage(List<AlluxioURI> inputFiles, List<AlluxioURI> outputFiles, Job job)
      throws InvalidPathException, FileAlreadyExistsException, BlockInfoException, IOException,
      AccessControlException, FileDoesNotExistException;

  /**
   * Deletes a lineage.
   *
   * @param lineageId id the of lineage
   * @param cascade the flag if to delete all the downstream lineages
   * @return true if the lineage is deleted, false otherwise
   * @throws LineageDoesNotExistException the lineage does not exist
   * @throws LineageDeletionException the lineage deletion fails
   */
  boolean deleteLineage(long lineageId, boolean cascade)
      throws LineageDoesNotExistException, LineageDeletionException;

  /**
   * Reinitializes the file when the file is lost or not completed.
   *
   * @param path the path to the file
   * @param blockSizeBytes the block size
   * @param ttl the TTL
   * @param ttlAction action to perform on ttl expiry
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise
   * @throws InvalidPathException the file path is invalid
   * @throws LineageDoesNotExistException when the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws FileDoesNotExistException if the path does not exist
   */
  long reinitializeFile(String path, long blockSizeBytes, long ttl, TtlAction ttlAction)
      throws InvalidPathException, LineageDoesNotExistException, AccessControlException,
      FileDoesNotExistException;

  /**
   * @return the list of all the {@link LineageInfo}s
   * @throws LineageDoesNotExistException if the lineage does not exist
   * @throws FileDoesNotExistException if any associated file does not exist
   */
  List<LineageInfo> getLineageInfoList()
      throws LineageDoesNotExistException, FileDoesNotExistException;

  /**
   * Schedules persistence for the output files of the given checkpoint plan.
   *
   * @param plan the plan for checkpointing
   */
  void scheduleCheckpoint(CheckpointPlan plan);

  /**
   * Reports a file as lost.
   *
   * @param path the path to the file
   * @throws FileDoesNotExistException if the file does not exist
   * @throws AccessControlException if permission checking fails
   * @throws InvalidPathException if the path is invalid
   */
  void reportLostFile(String path)
      throws FileDoesNotExistException, AccessControlException, InvalidPathException;
}
