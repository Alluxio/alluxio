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
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.client.ClientContext;
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

import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link LineageClient} implementation. This class does not access the master client directly
 * but goes through the implementations provided in {@link AbstractLineageClient}.
 */
@PublicApi
@ThreadSafe
public final class AlluxioLineage extends AbstractLineageClient {
  private static AlluxioLineage sAlluxioLineage;

  /**
   * @return the current lineage for Alluxio
   */
  public static synchronized AlluxioLineage get() {
    if (sAlluxioLineage == null) {
      if (!ClientContext.getConf().getBoolean(Constants.USER_LINEAGE_ENABLED)) {
        throw new IllegalStateException("Lineage is not enabled in the configuration.");
      }
      sAlluxioLineage = new AlluxioLineage();
    }
    return sAlluxioLineage;
  }

  protected AlluxioLineage() {
    super();
  }

  /**
   * Convenience method for {@link #createLineage(List, List, Job, CreateLineageOptions)} with
   * default options.
   *
   * @param inputFiles the files that the job depends on
   * @param outputFiles the files that the job outputs
   * @param job the job that takes the listed input file and computes the output file
   * @return the lineage id
   * @throws FileDoesNotExistException an input file does not exist in Alluxio storage, nor is added
   *         as an output file of an existing lineage
   * @throws AlluxioException if an unexpected alluxio error occurs
   * @throws IOException if the master cannot create the lineage
   */
  public long createLineage(List<AlluxioURI> inputFiles, List<AlluxioURI> outputFiles, Job job)
      throws FileDoesNotExistException, AlluxioException, IOException {
    return createLineage(inputFiles, outputFiles, job, CreateLineageOptions.defaults());
  }

  /**
   * Convenience method for {@link #deleteLineage(long, DeleteLineageOptions)} with default options.
   *
   * @param lineageId the id of the lineage
   * @return true if the lineage deletion is successful, false otherwise
   * @throws IOException if the master cannot delete the lineage
   * @throws LineageDoesNotExistException if the lineage does not exist
   * @throws LineageDeletionException if the deletion is cascade but the lineage has children
   * @throws AlluxioException if an unexpected alluxio error occurs
   */
  public boolean deleteLineage(long lineageId)
      throws IOException, LineageDoesNotExistException, LineageDeletionException, AlluxioException {
    return deleteLineage(lineageId, DeleteLineageOptions.defaults());
  }

  /**
   * Convenience method for {@link #getLineageInfoList(GetLineageInfoListOptions)} with default
   * options.
   *
   * @return the information about lineages
   * @throws IOException if the master cannot list the lineage info
   */
  public List<LineageInfo> getLineageInfoList() throws IOException {
    return getLineageInfoList(GetLineageInfoListOptions.defaults());
  }
}
