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

package alluxio.worker.http;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.grpc.JobProgressReportFormat;
import alluxio.grpc.LoadJobPOptions;
import alluxio.job.JobDescription;
import alluxio.job.LoadJobRequest;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.Optional;

/**
 * This service allows to submit, stop, and get the progress of the load job by HTTP RESTful API.
 */
public class HttpLoadService {

  enum JobType {
    LOAD("load");

    private String mTypeStr;

    JobType(String typeStr) {
      mTypeStr = typeStr;
    }

    public String getTypeString() {
      return mTypeStr;
    }
  }

  private static final JobProgressReportFormat DEFAULT_FORMAT = JobProgressReportFormat.TEXT;

  private final FileSystem mFileSystem;

  /**
   * This service allows to submit, stop, and get the progress of the load job by HTTP RESTful API.
   * @param fs the file system to submit, stop and get the progress of the load job
   */
  public HttpLoadService(FileSystem fs) {
    mFileSystem = fs;
  }

  /**
   * This method is for submitting, stopping and getting the progress of a load job.
   * There are three types of operations:
   * <hr/>
   * <h3>SUBMIT:</h3>
   * <p>description: submit a load job</p>
   * <p>example:</p>
   * <p>http://localhost:28080/v1/load?path=s3a://example-bucket/&opType=submit&partialListing=true
   * &verify=true&loadMetadataOnly=true&verbose=true&skipIfExists=true</p>
   * <hr/>
   * <h3>STOP:</h3>
   * <p>description: stop the load job</p>
   * <p>example:</p>
   * <p>http://localhost:28080/v1/load?path=s3a://example-bucket/&opType=stop</p>
   * <hr/>
   * <h3>PROGRESS:</h3>
   * <p>description: get the progress of the load job</p>
   * <p>example:</p>
   * <p>http://localhost:28080/v1/load?path=s3a://example-bucket/&opType=progress
   * &progressFormat=text&verbose=true</p>
   * <hr/>
   * @param path the path for loading
   * @param loadOptions the load options for the load job
   * @return the response string returned from the service
   */
  public String load(AlluxioURI path, HttpLoadOptions loadOptions) {
    switch (loadOptions.getOpType()) {
      case SUBMIT:
        return submitLoad(path, loadOptions);
      case STOP:
        return stopLoad(path);
      case PROGRESS:
        return getProgress(path, loadOptions);
      default:
        throw new UnsupportedOperationException("Unsupported op type: " + loadOptions.getOpType());
    }
  }

  private String submitLoad(AlluxioURI path, HttpLoadOptions loadOptions) {
    LoadJobPOptions.Builder options = alluxio.grpc.LoadJobPOptions
        .newBuilder().setPartialListing(loadOptions.isPartialListing())
        .setVerify(loadOptions.isVerify())
        .setLoadMetadataOnly(loadOptions.isLoadMetadataOnly())
        .setSkipIfExists(loadOptions.isSkipIfExists());
    if (loadOptions.getBandwidth().isPresent()) {
      long bandWidth = loadOptions.getBandwidth().getAsLong();
      if (bandWidth > 0L) {
        options.setBandwidth(bandWidth);
      }
    }
    LoadJobRequest job = new LoadJobRequest(path.toString(), options.build());
    try {
      Optional<String> jobId = mFileSystem.submitJob(job);
      if (jobId.isPresent()) {
        return String.format("Load '%s' is successfully submitted. JobId: %s%n", path, jobId.get());
      } else {
        return String.format("Load already running for path '%s' %n", path);
      }
    } catch (StatusRuntimeException e) {
      return String.format("Failed to submit load job " + path + ": " + e.getMessage());
    }
  }

  private String stopLoad(AlluxioURI path) {
    try {
      if (mFileSystem.stopJob(JobDescription
          .newBuilder()
          .setPath(path.toString())
          .setType(JobType.LOAD.getTypeString())
          .build())) {
        return String.format("Load '%s' is successfully stopped.%n", path);
      } else {
        return String.format("Cannot find load job for path %s, it might have already been "
            + "stopped or finished%n", path);
      }
    } catch (StatusRuntimeException e) {
      return String.format("Failed to stop load job " + path + ": " + e.getMessage());
    }
  }

  private String getProgress(AlluxioURI path, HttpLoadOptions loadOptions) {
    JobProgressReportFormat format = DEFAULT_FORMAT;
    if (loadOptions.getProgressFormat() != null && !loadOptions.getProgressFormat().isEmpty()) {
      format = JobProgressReportFormat.valueOf(loadOptions.getProgressFormat());
    }
    return getProgressInternal(path, format, loadOptions.isVerbose());
  }

  private String getProgressInternal(AlluxioURI path, JobProgressReportFormat format,
                                  boolean verbose) {
    try {
      return "Progress for loading path '" + path + "':\n"
          + (mFileSystem.getJobProgress(JobDescription
              .newBuilder()
              .setPath(path.toString())
              .setType(JobType.LOAD.getTypeString())
              .build(), format, verbose));
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return "Load for path '" + path + "' cannot be found.";
      }
      return "Failed to get progress for load job " + path + ": " + e.getMessage();
    }
  }
}
