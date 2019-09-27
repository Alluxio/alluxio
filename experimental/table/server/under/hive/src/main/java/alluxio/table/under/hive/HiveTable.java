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

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.conf.ServerConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.grpc.catalog.FieldSchema;
import alluxio.grpc.catalog.FileStatistics;
import alluxio.grpc.catalog.HiveTableInfo;
import alluxio.grpc.catalog.ParquetMetadata;
import alluxio.grpc.catalog.PartitionInfo;
import alluxio.grpc.catalog.Schema;
import alluxio.grpc.catalog.UdbTableInfo;
import alluxio.table.common.TableView;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.under.hive.parquet.AlluxioInputFile;
import alluxio.table.under.hive.util.PathTranslator;
import alluxio.util.ConfigurationUtils;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;
import org.apache.thrift.TException;
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

  private final HiveMetaStoreClient mHive;
  private final HiveDatabase mHiveDatabase;
  private final PathTranslator mPathTranslator;
  private final String mName;
  private final Schema mSchema;
  private final String mBaseLocation;
  private final Map<String, FileStatistics> mStatistics;
  private final List<FieldSchema> mPartitionKeys;
  private final Table mTable;

  /**
   * Creates a new instance.
   *
   * @param hive the hive client
   * @param hiveDatabase the hive db
   * @param pathTranslator the path translator
   * @param name the table name
   * @param schema the table schema
   * @param baseLocation the base location
   * @param statistics the table statistics
   * @param cols partition keys
   * @param partitions partition list
   * @param table hive table object
   */
  public HiveTable(HiveMetaStoreClient hive, HiveDatabase hiveDatabase,
      PathTranslator pathTranslator, String name, Schema schema, String baseLocation,
      Map<String, FileStatistics> statistics, List<FieldSchema> cols, List<Partition> partitions,
      Table table) throws IOException {
    // TODO(gpang): don't throw exception in constructor
    mHive = hive;
    mHiveDatabase = hiveDatabase;
    mPathTranslator = pathTranslator;
    mName = name;
    mSchema = schema;
    mBaseLocation = baseLocation;
    mStatistics = statistics;
    mPartitionKeys = cols;
    mTable = table;
  }

  // TODO(yuzhu): clean this up to use proper method to get a list of datafiles
  private static Map<String, ParquetMetadata> getPartitionMetadata(String path,
      FileSystem alluxioFs) {
    Map<String, ParquetMetadata> metadataMap = new HashMap<>();
    try {
      for (URIStatus status : alluxioFs.listStatus(new AlluxioURI(path))) {
        if (!status.isFolder() && !status.getName().endsWith(".crc")
            && !status.getName().equals("_SUCCESS")) {
          // it is a data file
          org.apache.parquet.hadoop.metadata.ParquetMetadata footer = null;
          try {
            InputFile in = AlluxioInputFile.create(alluxioFs, status);
            try (ParquetFileReader reader = ParquetFileReader.open(in)) {
              footer = reader.getFooter();
            }
          } catch (IOException | RuntimeException e) {
            LOG.warn("Unable to read parquet footer {}", status.getPath(), e);
          }
          if (footer != null) {
            String alluxioFilePath = status.getPath();
            if (alluxioFilePath.startsWith("/")) {
              // prefix with the scheme and authority
              alluxioFilePath = ConfigurationUtils.getSchemeAuthority(ServerConfiguration.global())
                  + alluxioFilePath;
            }
            metadataMap.put(alluxioFilePath, HiveUtils.toProto(footer));
          }
        }
      }
    } catch (IOException | AlluxioException e) {
      LOG.warn("Unable to read parquet footer from partition location {}", path, e);
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
    return new HiveTableView(mBaseLocation, mStatistics, mPartitionKeys);
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
  public List<UdbPartition> getPartitions() throws IOException {
    List<UdbPartition> udbPartitions = new ArrayList<>();
    try {
      List<Partition> partitions = mHive.listPartitions(mHiveDatabase.getName(), mName, (short) -1);
      for (Partition partition : partitions) {
        String partName = Warehouse.makePartName(mTable.getPartitionKeys(), partition.getValues());
        PartitionInfo.Builder pib = PartitionInfo.newBuilder()
            .setDbName(partition.getDbName()).setTableName(mName)
            .addAllCols(HiveUtils.toProto(partition.getSd().getCols()))
            .setStorage(HiveUtils.toProto(partition.getSd(), mPathTranslator))
            .putAllFileMetadata(getPartitionMetadata(
                mPathTranslator.toAlluxioPath(partition.getSd().getLocation()),
                mHiveDatabase.getUdbContext().getFileSystem()))
            .setPartitionName(partName);
        if (partition.getValues() != null) {
          pib.addAllValues(partition.getValues());
        }
        udbPartitions.add(new HivePartition(new HiveLayout(pib.build())));
      }
      return udbPartitions;
    } catch (TException e) {
      throw new IOException(
          "failed to list hive partitions for table: " + mHiveDatabase.getName() + "." + mName, e);
    }
  }

  @Override
  public UdbTableInfo toProto() throws IOException {
    HiveTableInfo.Builder builder = HiveTableInfo.newBuilder();
    builder.setDatabaseName(mTable.getDbName()).setTableName(mTable.getTableName())
        .setOwner(mTable.getOwner()).setTableType(mTable.getTableType());

    StorageDescriptor sd = mTable.getSd();
    builder.addAllDataColumns(HiveUtils.toProto(mTable.getSd().getCols()))
        .addAllPartitionColumns(HiveUtils.toProto(mTable.getPartitionKeys()))
        .setStorage(HiveUtils.toProto(sd, mPathTranslator))
        .putAllParameters(mTable.getParameters());
    if (mTable.getViewOriginalText() != null) {
      builder.setViewOriginalText(mTable.getViewOriginalText());
    }
    if (mTable.getViewExpandedText() != null) {
      builder.setViewExpandedText(mTable.getViewExpandedText());
    }
    return UdbTableInfo.newBuilder().setHiveTableInfo(builder.build()).build();
  }
}
