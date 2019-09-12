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

package alluxio.table.under.hive;

import alluxio.grpc.FieldSchema;
import alluxio.grpc.FileStatistics;
import alluxio.grpc.ParquetMetadata;
import alluxio.grpc.PartitionInfo;
import alluxio.grpc.Schema;
import alluxio.table.common.TableView;
import alluxio.table.common.udb.UdbTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hive table implementation.
 */
public class HiveTable implements UdbTable {
  private static final Logger LOG = LoggerFactory.getLogger(HiveTable.class);

  private final String mName;
  private final Schema mSchema;
  private final String mBaseLocation;
  private final Map<String, FileStatistics> mStatistics;
  private final List<PartitionInfo> mPartitionInfo;
  private final List<FieldSchema> mPartitionKeys;
  private final Hive mHive;

  /**
   * Creates a new instance.
   *
   * @param hive the hive client
   * @param name the table name
   * @param schema the table schema
   * @param baseLocation the base location
   * @param statistics the table statistics
   * @param cols partition keys
   * @param partitions partition list
   */
  public HiveTable(Hive hive, String name, Schema schema, String baseLocation,
      Map<String, FileStatistics> statistics, List<FieldSchema> cols,
      List<Partition> partitions) {
    mHive = hive;
    mName = name;
    mSchema = schema;
    mBaseLocation = baseLocation;
    mStatistics = statistics;
    mPartitionKeys = cols;
    mPartitionInfo = new ArrayList<>();
    for (Partition part: partitions) {
      PartitionInfo.Builder pib = PartitionInfo.newBuilder()
          .setTableName(mName)
          .setSd(part.getLocation())
          .putAllFileMetadata(getPartitionMetadata(part.getPartitionPath(), mHive));
      if (part.getValues() != null) {
        pib.addAllValues(part.getValues());
      }
      mPartitionInfo.add(pib.build());
    }
  }

  // TODO(yuzhu): clean this up to use proper method to get a list of datafiles
  private static Map<String, ParquetMetadata> getPartitionMetadata(Path path, Hive hive) {
    Map<String, ParquetMetadata> metadataMap = new HashMap<>();
    try {
      FileSystem fs = path.getFileSystem(hive.getConf());
      for (FileStatus status : fs.listStatus(path)) {
        if (status.isFile() && !status.getPath().getName().endsWith(".crc")
            && !status.getPath().getName().equals("_SUCCESS")) {
          // it is a data file
          org.apache.parquet.hadoop.metadata.ParquetMetadata footer = null;
          try {
            InputFile in = HadoopInputFile.fromPath(status.getPath(), new Configuration());
            try (ParquetFileReader reader = ParquetFileReader.open(in)) {
              footer = reader.getFooter();
            }
          } catch (IOException | RuntimeException e) {
            LOG.warn("Unable to read parquet footer {}, exception {}", status.getPath(), e);
          }
          if (footer != null) {
            metadataMap.put(status.getPath().toString(), HiveUtils.toProto(footer));
          }
        }
      }
    } catch (IOException e) {
      LOG.warn("Unable to read parquet footer from partition location {}", path.toString());
    }
    return metadataMap;
  }

  @Override
  public String getName() {
    return mName;
  }

  @Override
  public Schema getSchema() {
    return mSchema;
  }

  @Override
  public TableView getView() {
    return new HiveTableView(mBaseLocation, mStatistics, mPartitionKeys, getPartitions());
  }

  @Override
  public String getBaseLocation() {
    return mBaseLocation;
  }

  @Override
  public Map<String, FileStatistics> getStatistics() {
    return mStatistics;
  }

  @Override
  public List<PartitionInfo> getPartitions() {
    return mPartitionInfo;
  }
}
