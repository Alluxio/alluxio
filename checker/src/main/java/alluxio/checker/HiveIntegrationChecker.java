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

package alluxio.checker;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Some Hive queries to test the integration of Hive with Alluxio.
 * Supports two options: Alluxio is used as storage of Hive tables
 * and Alluxio is configured as Hive default filesytem.
 *
 * This checker requires a running Hive cluster and a running Alluxio cluster.
 * It will check whether Alluxio classes and Alluxio filesystem can be recognized
 * in Hive queries.
 */
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public class HiveIntegrationChecker {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIntegrationChecker.class);
  private static final String FAIL_TO_FIND_CLASS_MESSAGE = "Please distribute "
      + "the Alluxio client jar on the classpath of the application across different nodes.\n\n"
      + "Please set HIVE_AUX_JARS_PATH either in shell or in conf/hive-env.sh "
      + "before starting hiveserver2.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Running-Hive-with-Alluxio.html\n";
  private static final String FAIL_TO_FIND_FS_MESSAGE = "Please check the fs.alluxio.impl property "
      + "in core-site.xml file of your Hadoop installation.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Debugging-Guide.html\n";
  private static final String TEST_FAILED_MESSAGE = "***** Integration test failed. *****\n";
  private static final String TEST_PASSED_MESSAGE = "***** Integration test passed. *****\n";

  @Parameter(names = {"-alluxioUrl"}, description = "the alluxio cluster url of form "
      + "alluxio://master_hostname:port, which is converted from "
      + "the alluxio.master.hostname property in alluxio-site.properties")
  private String mAlluxioURL = "";

  @Parameter(names = {"-mode"}, description = "storage means Alluxio is used as storage of "
      + "Hive tables, dfs means Alluxio is configured as Hive default filesytem.")
  private String mUserMode = "storage";

  @Parameter(names = {"-hiveUrl", "-hiveurl"}, description = "a Hive connection url "
      + "of the form jdbc:subprotocol:subname", required = true)
  private String mHiveURL;

  @Parameter(names = {"-user"}, description = "the Hive user on whose behalf "
      + "the connection is being made")
  private String mHiveUserName = System.getProperty("user.name");

  @Parameter(names = {"-password"}, description = "the Hive user's password")
  private String mHiveUserPassword = "";

  /**
   * Implements Hive with Alluxio integration checker.
   *
   * @param reportWriter save user-facing messages to a generated file
   * @return 0 is success, 1 otherwise
   */
  private int run(PrintWriter reportWriter) {
    // Try to connect to Hive through JDBC
    try (Connection con = DriverManager.getConnection(mHiveURL, mHiveUserName, mHiveUserPassword)) {
      // Hive statements to check the integration
      String tableName = "hiveTestTable";
      String sql = String.format("drop table if exists %s", tableName);
      try (PreparedStatement dropTablePS = con.prepareStatement(sql)) {
        dropTablePS.execute();
      }

      // Creates test table based on different integration ways
      if (mUserMode.equals("storage")) {
        useAsSource(con, tableName);
      } else {
        useAsUnderFS(con, tableName);
      }

      sql = String.format("describe %s", tableName);
      try (PreparedStatement describeTablePS = con.prepareStatement(sql)) {
        describeTablePS.execute();
      }

      sql = String.format("select * from %s", tableName);
      try (PreparedStatement selectTablePS = con.prepareStatement(sql)) {
        selectTablePS.execute();
      }
    } catch (Exception e) {
      printExceptionReport(e, reportWriter);
      return 1;
    }
    reportWriter.println("Congratulations, you have configured Hive with Alluxio correctly!\n");
    reportWriter.println(TEST_PASSED_MESSAGE);
    return 0;
  }

  /**
   * Supports the first integration way: use Alluxio as one option to store Hive tables.
   *
   * @param con the Hive connection
   * @param tableName the name of the test table
   */
  private void useAsSource(Connection con, String tableName) throws Exception {
    String sql = String.format("CREATE TABLE %s (ROW1 STRING, ROW2 STRING) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
        + "LOCATION '%s/alluxioTestFolder/'", tableName, mAlluxioURL);
    try (PreparedStatement createTablePS = con.prepareStatement(sql)) {
      createTablePS.execute();
    }
  }

  /**
   * Supports the second integration way: use Alluxio as the default filesystem.
   *
   * @param con the Hive connection
   * @param tableName the name of the test table
   */
  private void useAsUnderFS(Connection con, String tableName) throws Exception {
    String sql = String.format("CREATE TABLE %s (ROW1 STRING, ROW2 STRING) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
        + "STORED AS TEXTFILE", tableName);
    try (PreparedStatement createTablePS = con.prepareStatement(sql)) {
      createTablePS.execute();
    }

    sql = String.format("INSERT INTO %s VALUES ('Hive', 'Test')", tableName);
    try (PreparedStatement loadTablePS = con.prepareStatement(sql)) {
      loadTablePS.executeUpdate();
    }
  }

  /**
   * Translates the exception message to user-facing message.
   *
   * @param exception the thrown exception when running Hive queries
   * @param reportWriter save user-facing messages to a generated file
   */
  private void printExceptionReport(Exception exception, PrintWriter reportWriter) {
    String exceptionStr = exception.toString();
    if (exceptionStr.contains("Class alluxio.hadoop.FileSystem not found")) {
      reportWriter.println(FAIL_TO_FIND_CLASS_MESSAGE);
    } else if (exceptionStr.contains("No FileSystem for scheme \"alluxio\"")
        || exceptionStr.contains("No FileSystem for scheme: alluxio")) {
      reportWriter.println(FAIL_TO_FIND_FS_MESSAGE);
    } else {
      reportWriter.println("Please fix the following error and "
          + "rerun the Hive Integration Checker. \n");
      exception.printStackTrace(reportWriter);
    }
    reportWriter.println(TEST_FAILED_MESSAGE);
  }

  /** Checks if input arguments are valid. */
  private void checkIfInputValid() {
    String modeInvalidMessage = "\n-mode <USER_MODE>  USER_MODE should be \"storage\" or \"dfs\".\n"
        + "Please check your USER_MODE and rerun the checker.";
    Preconditions.checkArgument(mUserMode.equals("storage") || mUserMode.equals("dfs"),
        modeInvalidMessage);

    String hiveUrlInvalidMessage = "\n-hiveurl <HIVE_URL> is a Hive Url of form "
        + "jdbc:subprotocol:subname.\nPlease check your HIVE_URL and rerun the checker.";
    Preconditions.checkArgument(mHiveURL.startsWith("jdbc:"),
        hiveUrlInvalidMessage);
  }

  /**
   * Main function will be triggered by hive-checker.sh.
   *
   * @param args the arguments to connect to Hive and define the test mode
   */
  public static void main(String[] args) throws Exception {
    HiveIntegrationChecker checker = new HiveIntegrationChecker();
    JCommander jCommander = new JCommander(checker, args);
    jCommander.setProgramName("HiveIntegrationChecker");

    try {
      checker.checkIfInputValid();
    } catch (Exception e) {
      jCommander.usage();
      System.out.println(e);
      System.exit(1);
    }

    try (PrintWriter reportWriter = CheckerUtils.initReportFile()) {
      int result = checker.run(reportWriter);
      reportWriter.flush();
      System.exit(result);
    }
  }
}
