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

package alluxio.master.table;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.Schema;
import alluxio.grpc.table.TableInfo;
import alluxio.proto.journal.Table.AddTableEntry;
import alluxio.proto.journal.Table.AddTablePartitionsEntry;
import alluxio.table.common.UdbPartition;
import alluxio.table.common.transform.TransformContext;
import alluxio.table.common.transform.TransformDefinition;
import alluxio.table.common.transform.TransformPlan;
import alluxio.table.common.udb.UdbTable;
import alluxio.util.CommonUtils;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * The table implementation which manages all the versions of the table.
 */
@NotThreadSafe
public class Table {
  private static final Logger LOG = LoggerFactory.getLogger(Table.class);
  private static final long UNDEFINED_VERSION = -1;
  private static final int PARTITIONS_CHUNK_SIZE = ServerConfiguration
      .getInt(PropertyKey.TABLE_JOURNAL_PARTITIONS_CHUNK_SIZE);

  public static final long FIRST_VERSION = 1;

  private final Database mDatabase;
  private final String mName;
  private final long mVersion;
  private final long mVersionCreationTime;
  private final long mPreviousVersion;

  private final Schema mSchema;
  private final PartitionScheme mPartitionScheme;
  private final String mOwner;
  private final List<ColumnStatisticsInfo> mStatistics;
  private final Map<String, String> mParameters;

  /**
   * @param database the database
   * @param udbTable the udb table to sync from
   * @param previousTable the previous table, or {@code null} if creating first version of table
   */
  private Table(Database database, UdbTable udbTable, @Nullable Table previousTable) {
    mDatabase = database;
    mVersion = previousTable == null ? FIRST_VERSION : previousTable.mVersion + 1;
    mPreviousVersion = previousTable == null ? UNDEFINED_VERSION : previousTable.mVersion;
    mVersionCreationTime = CommonUtils.getCurrentMs();

    mName = udbTable.getName();
    mSchema = udbTable.getSchema();
    mOwner = udbTable.getOwner() == null ? "" : udbTable.getOwner();
    mStatistics = udbTable.getStatistics();
    mParameters = new HashMap<>(udbTable.getParameters());

    // TODO(gpang): inspect listing of table or partition location?

    // Compare udb partitions with the existing partitions
    List<Partition> partitions = new ArrayList<>(udbTable.getPartitions().size());
    if (previousTable != null) {
      // spec to existing partition
      Map<String, Partition> existingPartitions =
          previousTable.mPartitionScheme.getPartitions().stream()
              .collect(Collectors.toMap(Partition::getSpec, Function.identity()));
      for (UdbPartition udbPartition : udbTable.getPartitions()) {
        Partition newPartition = existingPartitions.get(udbPartition.getSpec());
        if (newPartition == null) {
          // partition does not exist yet
          newPartition = new Partition(udbPartition);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Existing table {}.{} adding UDB partition: {}",
                database.getName(), mName, udbPartition.toString());
          }
        } else if (!newPartition.getBaseLayout().equals(udbPartition.getLayout())) {
          // existing partition is updated
          newPartition = newPartition.createNext(udbPartition);
          if (LOG.isDebugEnabled()) {
            LOG.debug("Existing table {}.{} updating UDB partition {}",
                database.getName(), mName, udbPartition.toString());
          }
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Existing table {}.{} keeping partition spec: {}",
                database.getName(), mName, udbPartition.toString());
          }
        }
        partitions.add(newPartition);
      }
      LOG.info("Updating existing table {}.{} with {} total partitions.",
          database.getName(), mName, partitions.size());
    } else {
      // Use all the udb partitions
      partitions =
          udbTable.getPartitions().stream().map(Partition::new).collect(Collectors.toList());
      LOG.info("Creating new table {}.{} with {} total partitions.",
          database.getName(), mName, partitions.size());
      if (LOG.isDebugEnabled()) {
        udbTable.getPartitions().stream().forEach(udbPartition ->
            LOG.debug("New table {}.{} adding UDB partition: {}.",
                database.getName(), mName, udbPartition.toString()));
      }
    }
    mPartitionScheme =
        PartitionScheme.create(partitions, udbTable.getLayout(), udbTable.getPartitionCols());
  }

  private Table(Database database, alluxio.proto.journal.Table.AddTableEntry entry) {
    List<Partition> partitions = entry.getPartitionsList().stream()
        .map(p -> Partition.fromProto(database.getContext().getLayoutRegistry(), p))
        .collect(Collectors.toList());

    mDatabase = database;
    mName = entry.getTableName();
    mPreviousVersion = entry.getPreviousVersion();
    mVersion = entry.getVersion();
    mVersionCreationTime = entry.getVersionCreationTime();
    mSchema = entry.getSchema();
    mPartitionScheme =
        PartitionScheme.create(partitions, entry.getLayout(), entry.getPartitionColsList());
    mOwner = entry.getOwner();
    mStatistics = entry.getTableStatsList();
    mParameters = new HashMap<>(entry.getParametersMap());
  }

  /**
   * @param database the database
   * @param udbTable the udb table
   * @param previousTable the previous table, or {@code null} if creating first version of table
   * @return a new (or first) version of the table based on the udb table, or {@code null} if there
   *         no changes in the udb table
   */
  public static Table create(Database database, UdbTable udbTable, @Nullable Table previousTable) {
    if (previousTable != null && !previousTable.shouldSync(udbTable)) {
      // no need for a new version
      return null;
    }
    return new Table(database, udbTable, previousTable);
  }

  /**
   * @param database the database
   * @param entry the add table journal entry
   * @return a new instance
   */
  public static Table create(Database database, alluxio.proto.journal.Table.AddTableEntry entry) {
    return new Table(database, entry);
  }

  /**
   * Add partitions to the current table.
   *
   * @param entry the add table partitions entry
   */
  public void addPartitions(alluxio.proto.journal.Table.AddTablePartitionsEntry entry) {
    mPartitionScheme.addPartitions(entry.getPartitionsList().stream()
        .map(p -> Partition.fromProto(mDatabase.getContext().getLayoutRegistry(), p))
        .collect(Collectors.toList()));
  }

  /**
   * @return the table name
   */
  public String getName() {
    return mName;
  }

  /**
   * @param spec the partition spec
   * @return the corresponding partition
   */
  public Partition getPartition(String spec) {
    return mPartitionScheme.getPartition(spec);
  }

  /**
   * @return the list of partitions
   */
  public List<Partition> getPartitions() {
    return mPartitionScheme.getPartitions();
  }

  /**
   * @return the table schema
   */
  public Schema getSchema() {
    return mSchema;
  }

  /**
   * @return the statistics
   */
  public List<ColumnStatisticsInfo> getStatistics() {
    return mStatistics;
  }

  /**
   * @return the table version
   */
  public long getVersion() {
    return mVersion;
  }

  /**
   * @return the previous table version
   */
  public long getPreviousVersion() {
    return mPreviousVersion;
  }

  /**
   * Returns a list of plans to transform the table, according to the transformation definition.
   *
   * @param definition the transformation definition
   * @return a list of {@link TransformPlan} to transform this table
   */
  public List<TransformPlan> getTransformPlans(TransformDefinition definition) throws IOException {
    List<TransformPlan> plans = new ArrayList<>(getPartitions().size());
    for (Partition partition : getPartitions()) {
      if (!partition.isTransformed(definition.getDefinition())) {
        TransformContext transformContext =
            new TransformContext(mDatabase.getName(), mName, partition.getSpec());
        plans.add(partition.getTransformPlan(transformContext, definition));
      }
    }
    return plans;
  }

  /**
   * @param udbTable the udb table to check against
   * @return true if the table should be synced, because of differences in the udb table
   */
  public boolean shouldSync(UdbTable udbTable) {
    if (!Objects.equals(mName, udbTable.getName())
        || !Objects.equals(mSchema, udbTable.getSchema())
        || !Objects.equals(mOwner, udbTable.getOwner())
        || !Objects.equals(mStatistics, udbTable.getStatistics())
        || !Objects.equals(mParameters, udbTable.getParameters())) {
      // some fields are different
      return true;
    }

    Map<String, Partition> existingPartitions = mPartitionScheme.getPartitions().stream()
        .collect(Collectors.toMap(Partition::getSpec, Function.identity()));
    if (existingPartitions.size() != udbTable.getPartitions().size()) {
      return true;
    }

    for (UdbPartition udbPartition : udbTable.getPartitions()) {
      Partition newPartition = existingPartitions.get(udbPartition.getSpec());
      if (newPartition == null
          || !newPartition.getBaseLayout().equals(udbPartition.getLayout())) {
        // mismatch of a partition
        return true;
      }
    }

    return false;
  }

  /**
   * @return the proto representation
   */
  public TableInfo toProto() {
    TableInfo.Builder builder = TableInfo.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .setSchema(mSchema)
        .setOwner(mOwner)
        .putAllParameters(mParameters)
        .addAllPartitionCols(mPartitionScheme.getPartitionCols())
        .setLayout(mPartitionScheme.getTableLayout())
        .setPreviousVersion(mPreviousVersion)
        .setVersion(mVersion)
        .setVersionCreationTime(mVersionCreationTime);

    return builder.build();
  }

  /**
   * @return the journal proto representation
   */
  public AddTableEntry getTableJournalProto() {
    AddTableEntry.Builder builder = AddTableEntry.newBuilder()
        .setDbName(mDatabase.getName())
        .setTableName(mName)
        .addAllTableStats(mStatistics)
        .setSchema(mSchema)
        .setOwner(mOwner)
        .putAllParameters(mParameters)
        .addAllPartitionCols(mPartitionScheme.getPartitionCols())
        .setLayout(mPartitionScheme.getTableLayout())
        .setPreviousVersion(mPreviousVersion)
        .setVersion(mVersion)
        .setVersionCreationTime(mVersionCreationTime);

    List<Partition> partitions = getPartitions();
    if (partitions.size() <= PARTITIONS_CHUNK_SIZE) {
      builder.addAllPartitions(partitions.stream().map(Partition::toProto)
          .collect(Collectors.toList()));
    }
    return builder.build();
  }

  /**
   * @return the journal proto representation
   */
  public List<AddTablePartitionsEntry> getTablePartitionsJournalProto() {
    List<AddTablePartitionsEntry> partitionEntries = new ArrayList<>();
    List<Partition> partitions = getPartitions();
    if (partitions.size() <= PARTITIONS_CHUNK_SIZE) {
      return partitionEntries;
    }

    for (List<Partition> partitionChunk : Lists.partition(partitions, PARTITIONS_CHUNK_SIZE)) {
      partitionEntries.add(AddTablePartitionsEntry.newBuilder()
          .setDbName(mDatabase.getName())
          .setTableName(mName)
          .setVersion(mVersion)
          .addAllPartitions(partitionChunk.stream().map(Partition::toProto)
              .collect(Collectors.toList()))
          .build());
    }

    return partitionEntries;
  }
}
