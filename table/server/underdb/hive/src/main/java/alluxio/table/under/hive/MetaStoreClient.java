package alluxio.table.under.hive;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.List;

/**
 * Interface for the metastore.
 */
public interface MetaStoreClient {
  /**
   * Get all tables from the metastore.
   * @param dbname database name
   * @return a list of tables
   */
  List<String> getAllTables(String dbname) throws MetaException, IOException;

  /**
   * Get a table object from the metastore.
   * @param dbname database name
   * @param name table name
   * @return the table object
   */
  Table getTable(String dbname, String name) throws MetaException, TException,
      NoSuchObjectException, IOException;

  /**
   * Get a list of partitions for a specific table.
   * @param dbName database name
   * @param tblName table name
   * @param maxParts max number of partitions to return
   * @return a list of partitions
   */
  List<Partition> listPartitions(String dbName, String tblName, short maxParts)
      throws NoSuchObjectException, MetaException, TException, IOException;

  /**
   * Get a list of table column statistics.
   * @param dbName database name
   * @param tableName table name
   * @param colNames column name
   * @return a list of table column statistics
   */
  List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException,
      TException, InvalidInputException, InvalidObjectException, IOException;
}
