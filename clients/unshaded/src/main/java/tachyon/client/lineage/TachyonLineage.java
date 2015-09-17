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

import java.util.List;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.job.Job;

/**
 * Tachyon lineage client. This class is the entry point for all lineage related operations. An
 * instance of this class can be obtained via {@link TachyonLineage#get}.This class is thread safe.
 */
@PublicApi
public class TachyonLineage {
  /** the singleton */
  private static TachyonLineage sClient;

  /**
   * @return the only instance of lineage client.
   */
  public static synchronized TachyonLineage get() {
    if (sClient == null) {
      sClient = new TachyonLineage();
    }
    return sClient;
  }

  /**
   * Creates and adds a lineage. It requires all the input files must either exist in Tachyon
   * storage, or have been added as output files in other lineages. It also requires the output
   * files do not exist in Tachyon, and it will create an empty file for each of it.
   *
   * @param inputFiles the files that the job depends on
   * @param outputFiles the files that the job outputs
   * @param job the job to track
   * @return the dependency id
   */
  public long addLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles, Job job) {
    // TODO add lineage to master
    return -1L;
  }

  /**
   * Deletes a lineage identified by a given id. If the delete is cascade, it will delete all the
   * downstream lineages that depend on the given one recursively. Otherwise it does not delete the
   * lineage exception, if there are other lineages whose input files are the output files of the
   * specified lineage.
   *
   * @param lineageId the id of the lineage
   * @param cascade whether to delete all the downstream lineages recursively
   * @return true if the lineage deletion is successful, false otherwise
   */
  public boolean deleteLineage(long lineageId, boolean cascade) {
    // TODO delete lineage on master
    return false;
  }
}
