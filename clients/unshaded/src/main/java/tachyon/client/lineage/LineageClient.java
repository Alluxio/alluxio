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

package tachyon.client.lineage;

import java.io.IOException;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.lineage.options.DeleteLineageOptions;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.LineageDeletionException;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.exception.TachyonException;
import tachyon.job.Job;
import tachyon.thrift.LineageInfo;


/**
 * User facing interface for the Tachyon Lineage client APIs.
 */
@PublicApi
interface LineageClient {
  /**
   * Creates a lineage. It requires all the input files either exist in Tachyon storage, or have
   * been added as output files in other lineages. It also requires the output files do not exist in
   * Tachyon, and it will create an empty file for each of the output files.
   *
   * @param inputFiles the files that the job depends on
   * @param outputFiles the files that the job outputs
   * @param job the job that takes the listed input file and computes the output file
   * @return the lineage id
   * @throws IOException if the master cannot create the lineage
   * @throws FileDoesNotExistException an input file does not exist in Tachyon storage, nor is added
   *         as an output file of an existing lineage
   * @throws TachyonException if an unexpected tachyon error occurs
   */
  long createLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles, Job job)
      throws FileDoesNotExistException, IOException, TachyonException;

  /**
   * Lists all the lineages.
   *
   * @return the information about lineages
   * @throws IOException if the master cannot list the lineage info
   */
  public List<LineageInfo> getLineageInfoList() throws IOException;

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
   * @throws TachyonException if an unexpected tachyon error occurs
   */
  public boolean deleteLineage(long lineageId, DeleteLineageOptions options)
      throws IOException, LineageDoesNotExistException, LineageDeletionException, TachyonException;
}
