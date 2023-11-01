
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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.annotation.PublicApi;
import alluxio.client.file.DoraCacheFileSystem;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import alluxio.grpc.GetStatusPOptions;
import alluxio.grpc.LoadMetadataPType;
import alluxio.util.FormatUtils;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Moves a file or a directory in the Alluxio filesystem using job service.
 */
@ThreadSafe
@PublicApi
public final class CheckCachedCommand extends AbstractFileSystemCommand {
  private static final Logger LOG = LoggerFactory.getLogger(CheckCachedCommand.class);

  private static final int DEFAULT_LIMIT = 1000;
  private static final int LIMIT_WARNING_THRESHOLD = 10000;
  private static final int SAMPLE_FILE_SIZE = 100;
  private final FileSystem mUfsFileSystem;
  private final DoraCacheFileSystem mDoraCacheFileSystem;

  private static final Option SAMPLE_OPTION =
      Option.builder()
          .longOpt("sample")
          .required(false)
          .hasArg(true)
          .desc("Sample ratio, 10 means sample 1 in every 10 files.")
          .build();

  private static final Option LIMIT_OPTION =
      Option.builder()
          .longOpt("limit")
          .required(false)
          .hasArg(true)
          .desc("limit, default 1000")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public CheckCachedCommand(FileSystemContext fsContext) {
    super(fsContext);
    Preconditions.checkNotNull(mFileSystem.getDoraCacheFileSystem());
    mDoraCacheFileSystem = mFileSystem.getDoraCacheFileSystem();
    // Disable ufs fallback
    mDoraCacheFileSystem.setUfsFallbackEnabled(false);
    mUfsFileSystem =  mFileSystem.getUfsBaseFileSystem();
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    super.validateArgs(cl);
    String sampleOptionValue = cl.getOptionValue(SAMPLE_OPTION.getLongOpt());
    String limitOptionValue = cl.getOptionValue(LIMIT_OPTION.getLongOpt());
    if (sampleOptionValue != null) {
      Preconditions.checkState(StringUtils.isNumeric(sampleOptionValue));
    }
    if (limitOptionValue != null) {
      Preconditions.checkState(StringUtils.isNumeric(limitOptionValue));
      int limit = Integer.parseInt(limitOptionValue);
      if (limit > LIMIT_WARNING_THRESHOLD) {
        LOG.warn("Limit {} is too large. This may cause client freeze. "
            + "Considering reduce the value if the processing takes too long.", limit);
      }
    }
  }

  @Override
  public String getCommandName() {
    return "check-cached";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(SAMPLE_OPTION)
        .addOption(LIMIT_OPTION);
  }

  // TODO(elega) validate limit value
  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);

    int sampleRatio = 1;
    int limit = DEFAULT_LIMIT;

    int numFullyCachedFile = 0;
    int numPartiallyCachedFile = 0;
    int numNotCachedFile = 0;
    int numFailed = 0;

    long approximateCachedBytes = 0;
    long totalBytes = 0;

    List<String> fullyCachedFileSampleList = new ArrayList<>();
    List<String> partiallyCachedFileSampleList = new ArrayList<>();
    List<String> notCachedFileSampleList = new ArrayList<>();
    List<String> failedSampleList = new ArrayList<>();

    String sampleOptionValue = cl.getOptionValue(SAMPLE_OPTION.getLongOpt());
    String limitOptionValue = cl.getOptionValue(LIMIT_OPTION.getLongOpt());
    if (sampleOptionValue != null) {
      sampleRatio = Integer.parseInt(sampleOptionValue);
    }
    if (limitOptionValue != null) {
      limit = Integer.parseInt(limitOptionValue);
    }

    if (!mDoraCacheFileSystem.getStatus(path).isFolder()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_DIRECTORY.getMessage(path));
    }

    // TODO(elega) need an iterative API to avoid loading too many files
    List<URIStatus> statuses = mUfsFileSystem.listStatus(
            mDoraCacheFileSystem.convertToUfsPath(path)).stream()
        .filter(it -> !it.isFolder() && it.isCompleted()
    ).collect(Collectors.toList());
    Collections.shuffle(statuses);
    List<URIStatus> statusesToCheck =
        statuses.subList(0, Math.min(statuses.size() / sampleRatio, limit));
    for (URIStatus status: statusesToCheck) {
      String filePath = status.getUfsPath();
      try {
        URIStatus fileStatus = mDoraCacheFileSystem.getStatus(
            new AlluxioURI(filePath), GetStatusPOptions.newBuilder()
                .setLoadMetadataType(LoadMetadataPType.NEVER)
                    .setCommonOptions(FileSystemMasterCommonPOptions.newBuilder()
                        .setSyncIntervalMs(-1).build())
                .build()
        );
        totalBytes += fileStatus.getLength();
        approximateCachedBytes +=
            fileStatus.getLength() * fileStatus.getInAlluxioPercentage() / 100;
        if (fileStatus.getLength() == 0 || fileStatus.getInAlluxioPercentage() == 100) {
          numFullyCachedFile++;
          if (fullyCachedFileSampleList.size() < SAMPLE_FILE_SIZE) {
            fullyCachedFileSampleList.add(filePath);
          }
        } else if (fileStatus.getInAlluxioPercentage() > 0) {
          numPartiallyCachedFile++;
          if (partiallyCachedFileSampleList.size() < SAMPLE_FILE_SIZE) {
            partiallyCachedFileSampleList.add(filePath);
          }
        } else {
          numNotCachedFile++;
          if (notCachedFileSampleList.size() < SAMPLE_FILE_SIZE) {
            notCachedFileSampleList.add(filePath);
          }
        }
      } catch (Exception e) {
        if (e instanceof FileDoesNotExistException) {
          numNotCachedFile++;
          if (notCachedFileSampleList.size() < SAMPLE_FILE_SIZE) {
            notCachedFileSampleList.add(filePath);
          }
        } else {
          numFailed++;
          if (failedSampleList.size() < SAMPLE_FILE_SIZE) {
            failedSampleList.add(filePath);
          }
        }
      }
    }

    System.out.println("Total files checked: " + statusesToCheck.size());
    System.out.println("Fully cached files count: " + numFullyCachedFile);
    System.out.println("Partially cached files count: " + numPartiallyCachedFile);
    System.out.println("Not cached files count: " + numNotCachedFile);
    System.out.println("Failed files count: " + numFailed);
    System.out.println(
        "Total bytes checked: " + FormatUtils.getSizeFromBytes(totalBytes));
    System.out.println(
        "Approximate bytes cached: " + FormatUtils.getSizeFromBytes(approximateCachedBytes));
    System.out.println("--------------Fully cached files samples (up to 100)------------------");
    for (String file: fullyCachedFileSampleList) {
      System.out.println(file);
    }
    System.out.println();
    System.out.println("--------------Partially cached files samples (up to 100)-----------------");
    for (String file: partiallyCachedFileSampleList) {
      System.out.println(file);
    }
    System.out.println();
    System.out.println("--------------Not cached files samples (up to 100)------------------");
    for (String file: notCachedFileSampleList) {
      System.out.println(file);
    }
    System.out.println();
    System.out.println("--------------Failed files samples (up to 100)------------------");
    for (String file: failedSampleList) {
      System.out.println(file);
    }
    System.out.println();
    return 0;
  }

  @Override
  public String getUsage() {
    return "check-cached <path> [--sample <sample-ratio>] [--limit <limit-size>]";
  }

  @Override
  public String getDescription() {
    return "Checks if files under a path have been cached in alluxio.";
  }
}
