package alluxio.table.under.hive.util;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

/**
 * Hive compatibility.
 */
public interface HiveCompatibility {
  /**
   * Get table operation.
   * @param dbname database name
   * @param name table name
   * @return Table object
   */
  Table getTable(String dbname, String name)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Test if a table exists.
   * @param dbname database name
   * @param name table name
   * @return true if the table exists
   */
  boolean tableExists(String dbname, String name)
      throws MetaException, TException, NoSuchObjectException;
}
