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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.lineage.options.DeleteLineageOptions;
import tachyon.exception.FileDoesNotExistException;
import tachyon.exception.LineageDeletionException;
import tachyon.exception.LineageDoesNotExistException;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.thrift.LineageInfo;

/**
 * Tachyon Lineage client. This class provides implementation of interacting with Tachyon Lineage
 * master.
 */
@PublicApi
public abstract class AbstractLineageClient implements LineageClient {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  protected LineageContext mContext;

  @Override
  public long createLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles, Job job)
          throws FileDoesNotExistException, TachyonException, IOException {
    // TODO(yupeng): relax this to support other type of jobs
    Preconditions.checkState(job instanceof CommandLineJob, "only command line job supported");
    LineageMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long lineageId = masterClient.createLineage(stripURIList(inputFiles),
          stripURIList(outputFiles), (CommandLineJob) job);
      LOG.info("Created lineage " + lineageId);
      return lineageId;
    } catch (TachyonException e) {
      TachyonException.unwrap(e, FileDoesNotExistException.class);
      throw e;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public boolean deleteLineage(long lineageId, DeleteLineageOptions options) throws IOException,
      LineageDoesNotExistException, LineageDeletionException, TachyonException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.deleteLineage(lineageId, options.isCascade());
      LOG.info(result ? "Succeeded to " : "Failed to " + "delete lineage " + lineageId);
      return result;
    } catch (TachyonException e) {
      TachyonException.unwrap(e, LineageDoesNotExistException.class);
      TachyonException.unwrap(e, LineageDeletionException.class);
      throw e;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  @Override
  public List<LineageInfo> getLineageInfoList() throws IOException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();

    try {
      List<LineageInfo> result = masterClient.getLineageInfoList();
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Transfors the list of @{link TachyonURI} in a new list of Strings,
   * where each string is @{link TachyonURI#getPath()}
   * @param uris
   * @return
   */
  private List<String> stripURIList(List<TachyonURI> uris) {
    return Lists.transform(uris, new Function<TachyonURI, String>() {
      @Override
      public String apply(TachyonURI input) {
        return input.getPath();
      }
    });
  }
}
