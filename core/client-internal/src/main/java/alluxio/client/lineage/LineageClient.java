/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.lineage.options.CreateLineageOptions;
import alluxio.client.lineage.options.DeleteLineageOptions;
import alluxio.client.lineage.options.GetLineageInfoListOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.LineageDeletionException;
import alluxio.exception.LineageDoesNotExistException;
import alluxio.job.Job;
import alluxio.wire.LineageInfo;

import java.io.IOException;
import java.util.List;

/**
 * User facing interface for the Alluxio Lineage client APIs.
 */
@PublicApi
interface LineageClient {
  /**
   * Creates a lineage. It requires all the input files either exist in Alluxio storage, or have
   * been added as output files in other lineages. It also requires the output files do not exist in
   * Alluxio, and it will create an empty file for each of the output files.
   *
   * @param inputFiles the files that the job depends on
   * @param outputFiles the files that the job outputs
   * @param job the job that takes the listed input file and computes the output file
   * @param options the method options
   * @return the lineage id
   * @throws IOException if the master cannot create the lineage
   * @throws FileDoesNotExistException an input file does not exist in Alluxio storage, nor is added
   *         as an output file of an existing lineage
   * @throws AlluxioException if an unexpected alluxio error occurs
   */
  long createLineage(List<AlluxioURI> inputFiles, List<AlluxioURI> outputFiles, Job job,
      CreateLineageOptions options) throws FileDoesNotExistException, IOException, AlluxioException;

  /**
   * Lists all the lineages.
   *
   * @param options method options
   * @return the information about lineages
   * @throws IOException if the master cannot list the lineage info
   */
  public List<LineageInfo> getLineageInfoList(GetLineageInfoListOptions options) throws IOException;

  /**
   * Deletes a lineage identified by a given id. If the delete is cascade, it will delete all the
   * downstream lineages that depend on the given one recursively. Otherwise it throw a lineage
   * deletion exception, when there are other lineages whose input files are the output files of the
   * specified lineage.
   *
   * @param lineageId the id of the lineage
   * @param options method options
   * @return true if the lineage deletion is successful, false otherwise
   * @throws IOException if the master cannot delete the lineage
   * @throws LineageDeletionException if the deletion is cascade but the lineage has children
   * @throws LineageDoesNotExistException if the lineage does not exist
   * @throws AlluxioException if an unexpected alluxio error occurs
   */
  public boolean deleteLineage(long lineageId, DeleteLineageOptions options)
      throws IOException, LineageDoesNotExistException, LineageDeletionException, AlluxioException;
}
