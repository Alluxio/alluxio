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

package tachyon.client.table;

import java.io.IOException;
import java.nio.ByteBuffer;

import tachyon.TachyonURI;
import tachyon.annotation.PublicApi;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.exception.TachyonException;
import tachyon.thrift.RawTableInfo;

/**
 * User facing interface for the Tachyon Raw Table client APIs. Raw tables are collections of
 * columns stored as files. A column may consist of one or more ordered files.
 */
@PublicApi
interface TachyonRawTablesCore {
  /**
   * Creates a new raw table.
   *
   * @param path the path of the table to create in Tachyon space, this must not already exist as
   *             a file or table.
   * @param numColumns the number of columns in the table
   * @param metadata the metadata associated with the table, this will be stored as bytes and
   *                 should be in a format the user can later understand
   * @return a {@link RawTable} satisfying the input parameters
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  RawTable create(TachyonURI path, int numColumns, ByteBuffer metadata)
      throws IOException, TachyonException;

  /**
   * Creates a new partition in a column of a raw table. The partition is represented as a file,
   * and the user may interact with it through the FileSystem API. See {@link FileSystem}.
   *
   * @param column the raw column under which to create the partition
   * @param partitionId the index of the partition to create
   * @return a {@link FileOutStream} which may be used to write data into the partition
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  FileOutStream createPartition(RawColumn column, int partitionId)
      throws IOException, TachyonException;

  /**
   * Gets the metadata of a raw table, such as the number of columns.
   *
   * @param rawTable the handle for the table
   * @return the {@link RawTableInfo} for the table
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  RawTableInfo getInfo(RawTable rawTable) throws IOException, TachyonException;

  /**
   * Gets the number of partitions currently in the {@link RawColumn}. Each partition is a
   * separate file.
   *
   * @param column the raw column containing the partitions
   * @return the number of partitions currently in the column
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  int getPartitionCount(RawColumn column) throws IOException, TachyonException;

  /**
   * Gets the partition {@link TachyonURI} which can be used to access the partition data directly.
   *
   * @param column the column to get the partition from
   * @param partitionId the id of the partition
   * @return the uri referencing the partition
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  TachyonURI getPartitionUri(RawColumn column, int partitionId)
      throws IOException, TachyonException;

  /**
   * Gets a handle for the given raw table, if it exists.
   *
   * @param path the path of the table in Tachyon space
   * @return a {@link RawTable} representing the table
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  RawTable open(TachyonURI path) throws IOException, TachyonException;

  /**
   * Updates the user defined metadata for the raw table. This will overwrite the previous
   * metadata if it existed.
   *
   * @param rawTable the handle for the table
   * @param metadata the new metadata to associate with the table, this will be stored as bytes
   *                 and should be in a format the user can later understand
   * @throws IOException if a non Tachyon related I/O error occurs
   * @throws TachyonException if an internal Tachyon error occurs
   */
  void updateRawTableMetadata(RawTable rawTable, ByteBuffer metadata)
      throws IOException, TachyonException;
}
