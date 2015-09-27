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

import com.google.common.base.Preconditions;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.exception.TachyonException;
import tachyon.job.CommandLineJob;
import tachyon.job.Job;
import tachyon.thrift.FileDoesNotExistException;
import tachyon.thrift.LineageDeletionException;
import tachyon.thrift.LineageDoesNotExistException;
import tachyon.thrift.LineageInfo;

/**
 * Tachyon lineage client. This class is the entry point for all lineage related operations. An
 * instance of this class can be obtained via {@link TachyonLineageFileSystem#get}.
 */
@PublicApi
public class TachyonLineageFileSystem extends TachyonFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private static TachyonLineageFileSystem sTachyonFileSystem;
  private LineageContext mContext;

  public static synchronized TachyonLineageFileSystem get() {
    if (sTachyonFileSystem == null) {
      sTachyonFileSystem = new TachyonLineageFileSystem();
    }
    return sTachyonFileSystem;
  }

  protected TachyonLineageFileSystem() {
    super();
    mContext = LineageContext.INSTANCE;
  }

  /**
   * Creates a lineage. It requires all the input files must either exist in Tachyon storage, or
   * have been added as output files in other lineages. It also requires the output files do not
   * exist in Tachyon, and it will create an empty file for each of it.
   *
   * @param inputFiles the files that the job depends on
   * @param outputFiles the files that the job outputs
   * @param job the job to track
   * @return the dependency id
   * @throws IOException
   * @throws FileDoesNotExistException
   */
  public long createLineage(List<TachyonURI> inputFiles, List<TachyonURI> outputFiles, Job job)
      throws FileDoesNotExistException, IOException {
    // TODO(yupeng): relax this to support other type of jobs
    Preconditions.checkState(job instanceof CommandLineJob, "only command line job supported");

    LineageMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long lineageId = masterClient.createLineage(inputFiles, outputFiles, (CommandLineJob) job);
      LOG.info("Created lineage " + lineageId);
      return lineageId;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Deletes a lineage identified by a given id. If the delete is cascade, it will delete all the
   * downstream lineages that depend on the given one recursively. Otherwise it does not delete the
   * lineage exception, if there are other lineages whose input files are the output files of the
   * specified lineage.
   *
   * @param lineageId the id of the lineage
   * @param cascade whether to delete all the downstream lineages
   * @return true if the lineage deletion is successful, false otherwise
   * @throws IOException if the master cannot delete the lineage
   * @throws LineageDeletionException if the deletion is cascade but the lineage has children
   * @throws LineageDoesNotExistException if the lineage does not exist
   */
  public boolean deleteLineage(long lineageId, boolean cascade)
      throws IOException, LineageDoesNotExistException, LineageDeletionException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();
    try {
      boolean result = masterClient.deleteLineage(lineageId, cascade);
      LOG.info(result ? "Succeeded to " : "Failed to" + "delete lineage " + lineageId);
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Lists all the lineages.
   *
   * @return the informaiton about lineages
   * @throws IOException if the master cannot list the lineage info
   */
  public List<LineageInfo> listLineages() throws IOException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();

    try {
      List<LineageInfo> result = masterClient.listLineages();
      return result;
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * A file is created when its lineag is added. This method reinitializes the created file. But
   * it's no-op if the file is already completed.
   *
   * @return the id of the reinitialized file when the file is lost or not completed, -1 otherwise.
   * @throws LineageDoesNotExistException if the lineage does not exist.
   */
  public long recreate(TachyonURI path, OutStreamOptions options)
      throws LineageDoesNotExistException {
    LineageMasterClient masterClient = mContext.acquireMasterClient();
    try {
      long fileId =
          masterClient.recreateFile(path.getPath(), options.getBlockSize(), options.getTTL());
      return fileId;
    } catch (IOException e) {
      throw new RuntimeException("recreation failed", e);
    } finally {
      mContext.releaseMasterClient(masterClient);
    }
  }

  /**
   * Gets the output stream for lineage job. If the file already exists on master, returns a dummpy
   * output stream.
   */
  @Override
  public FileOutStream getOutStream(TachyonURI path, OutStreamOptions options)
      throws IOException, TachyonException {
    long fileId;
    try {
      fileId = recreate(path, options);
    } catch (LineageDoesNotExistException e) {
      // not a lineage file
      return super.getOutStream(path, options);
    }
    if (fileId < 0) {
      return new DummyFileOutputStream(fileId, options);
    }
    return new LineageFileOutStream(fileId, options);
  }
}
