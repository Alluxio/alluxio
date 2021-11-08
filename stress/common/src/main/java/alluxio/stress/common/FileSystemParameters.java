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

package alluxio.stress.common;

import alluxio.client.ReadType;
import alluxio.stress.Parameters;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * FileSystem common parameters.
 */
public class FileSystemParameters extends Parameters {
  @Parameter(names = {"--client-type"},
      description = "the client API type. Alluxio native or hadoop compatible client,"
          + " default is AlluxioHDFS",
      converter = FileSystemParameters.FileSystemParametersClientTypeConverter.class)
  public FileSystemClientType mClientType = FileSystemClientType.ALLUXIO_HDFS;

  @Parameter(names = {"--read-type"},
      description = "the cache mechanism during read. Options are [NONE, CACHE, CACHE_PROMOTE]"
          + " default is CACHE",
      converter = FileSystemParameters.FileSystemParametersReadTypeConverter.class)
  public ReadType mReadType = ReadType.CACHE;

  /**
   * @return FileSystemClientType of this bench
   * Converts from String to FileSystemClientType instance.
   */
  public static class FileSystemParametersClientTypeConverter
      implements IStringConverter<FileSystemClientType> {
    @Override
    public FileSystemClientType convert(String value) {
      return FileSystemClientType.fromString(value);
    }
  }

  /**
   * @return FileSystemClientType of this bench
   * Converts from String to FileSystemClientType instance.
   */
  public static class FileSystemParametersReadTypeConverter implements IStringConverter<ReadType> {
    @Override
    public ReadType convert(String value) {
      return ReadType.fromString(value);
    }
  }
}
