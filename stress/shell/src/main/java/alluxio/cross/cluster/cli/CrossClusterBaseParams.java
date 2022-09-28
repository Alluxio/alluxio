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

package alluxio.cross.cluster.cli;

import static alluxio.stress.BaseParameters.HELP_FLAG;

import alluxio.client.WriteType;
import alluxio.stress.common.FileSystemParameters;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.IParameterSplitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base parameters for cross cluster latency benchmarks.
 */
public class CrossClusterBaseParams {
  public static final String IP_LIST = "--ip-list";
  public static final String ROOT_PATH = "--path";
  public static final String SYNC_LATENCY = "--latency";
  public static final String WRITE_TYPE = "--write-type";
  @Parameter(names = {WRITE_TYPE},
      description = "The write type to use when creating files. Options are [MUST_CACHE, "
          + "CACHE_THROUGH, THROUGH, ASYNC_THROUGH]",
      converter = FileSystemParameters.FileSystemParametersWriteTypeConverter.class)
  public String mWriteType = WriteType.CACHE_THROUGH.toString();

  @Parameter(names = {SYNC_LATENCY},
      description = "Metadata sync latency")
  public int mSyncLatency = -1;

  @Parameter(names = {ROOT_PATH},
      description = "In alluxio path where to create and read files",
      required = true)
  public String mRootPath;

  @Parameter(names = {IP_LIST}, splitter = NoSplitter.class,
      description = "Each entry should be a comma seperated list of ip:port of the masters of each"
          + " cluster, this should be repeated for each cluster eg: --ip-list \"127.0.0.1:19998\""
          + " --ip-list \"127.0.0.1:19997,127.0.0.1:19996\"",
      required = true)
  public List<String> mClusterIps = new ArrayList<>();

  @Parameter(names = {"-h", HELP_FLAG}, help = true)
  public boolean mHelp = false;

  static class NoSplitter implements IParameterSplitter {
    public List<String> split(String value) {
      return Collections.singletonList(value);
    }
  }
}
