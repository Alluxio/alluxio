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

package alluxio.client.lineage;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.PropertyKey;
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

import javax.annotation.concurrent.ThreadSafe;

/**
 * A {@link LineageClient} implementation. This class does not access the master client directly
 * but goes through the implementations provided in {@link AbstractLineageClient}.
 */
@PublicApi
@ThreadSafe
public final class AlluxioLineage extends AbstractLineageClient {

  /**
   * @return the current lineage for Alluxio
   */
  public static synchronized AlluxioLineage get() {
    return get(LineageContext.INSTANCE);
  }

  /**
   * @param context lineage context
   * @return the current lineage for Alluxio
   */
  public static synchronized AlluxioLineage get(LineageContext context) {
    if (!Configuration.getBoolean(PropertyKey.USER_LINEAGE_ENABLED)) {
      throw new IllegalStateException("Lineage is not enabled in the configuration.");
    }
    return new AlluxioLineage(context);
  }

  /**
   * Internal constructor that constructs a new instance with a {@link LineageContext}.
   *
   * @param context lineage context
   */
  protected AlluxioLineage(LineageContext context) {
    super(context);
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
   * @throws LineageDoesNotExistException if the lineage does not exist
   * @throws LineageDeletionException if the deletion is cascade but the lineage has children
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
   */
  public List<LineageInfo> getLineageInfoList() throws IOException {
    return getLineageInfoList(GetLineageInfoListOptions.defaults());
  }
}
