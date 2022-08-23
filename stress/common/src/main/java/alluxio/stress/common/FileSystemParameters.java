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
import alluxio.client.WriteType;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.stress.Parameters;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

/**
 * FileSystem common parameters.
 */
public class FileSystemParameters extends Parameters {
  public static final String CLIENT_TYPE_OPTION_NAME = "--client-type";
  public static final String READ_TYPE_FLAG_OPTION_NAME = "--read-type";
  public static final String WRITE_TYPE_OPTION_NAME = "--write-type";

  // TODO(lu) will it show value value itself?
  @Parameter(names = {CLIENT_TYPE_OPTION_NAME},
      description = "the client API type. Alluxio native, hadoop compatible, "
          + "or POSIX client. Default is AlluxioHDFS",
      converter = FileSystemParameters.FileSystemParametersClientTypeConverter.class)
  public FileSystemClientType mClientType = FileSystemClientType.ALLUXIO_HDFS;

  @Parameter(names = {READ_TYPE_FLAG_OPTION_NAME},
      description = "the cache mechanism during read. Options are [NO_CACHE, CACHE, CACHE_PROMOTE]"
          + " default is CACHE",
      converter = FileSystemParameters.FileSystemParametersReadTypeConverter.class)
  public ReadType mReadType = ReadType.CACHE;

  @Parameter(names = {WRITE_TYPE_OPTION_NAME},
      description = "The write type to use when creating files. Options are [MUST_CACHE, "
          + "CACHE_THROUGH, THROUGH, ASYNC_THROUGH, ALL]",
      converter = FileSystemParameters.FileSystemParametersWriteTypeConverter.class)
  public String mWriteType = Configuration.getEnum(
      PropertyKey.USER_FILE_WRITE_TYPE_DEFAULT, WriteType.class).name();

  /**
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
   * Converts from String to FileSystemClientType instance.
   */
  public static class FileSystemParametersReadTypeConverter implements IStringConverter<ReadType> {
    @Override
    public ReadType convert(String value) {
      return ReadType.fromString(value);
    }
  }

  /**
   * Converts from String to a valid FileSystemWriteType String.
   */
  public static class FileSystemParametersWriteTypeConverter implements IStringConverter<String> {
    @Override
    public String convert(String value) {
      if (value.equals("MUST_CACHE")  || value.equals("CACHE_THROUGH")  || value.equals("THROUGH")
          || value.equals("ASYNC_THROUGH") || value.equals("ALL")) {
        return value;
      }
      throw new IllegalArgumentException(String.format("%s is not a valid write type. "
          + "Valid options are: MUST_CACHE, CACHE_THROUGH, THROUGH, ASYNC_THROUGH and ALL", value));
    }
  }
}
