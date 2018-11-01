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

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Some Hive queries to test the integration of Hive with Alluxio.
 * Supports two options: Alluxio is configured as Hive default filesystem
 * and Alluxio is used as a location of Hive tables other than Hive default filesystem.
 *
 * This checker requires a running Hive cluster and a running Alluxio cluster.
 * It will check whether Alluxio classes and Alluxio filesystem can be recognized
 * in Hive queries.
 */
@SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
public class HiveIntegrationChecker {
  private static final Logger LOG = LoggerFactory.getLogger(HiveIntegrationChecker.class);
  private static final String FAIL_TO_FIND_CLASS_MESSAGE = "Please set HIVE_AUX_JARS_PATH "
      + "either in shell or in conf/hive-env.sh before starting hiveserver2.\n\n"
      + "Please distribute the Alluxio client jar on the HIVE_AUX_JARS_PATH "
      + "of all Hive nodes.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Running-Hive-with-Alluxio.html\n";
  private static final String FAIL_TO_FIND_FS_MESSAGE = "Please check the fs.alluxio.impl property "
      + "in core-site.xml file of your Hadoop installation.\n\n"
      + "For details, please refer to: "
      + "https://www.alluxio.org/docs/master/en/Debugging-Guide.html\n";
  private static final String TEST_FAILED_MESSAGE = "***** Integration test failed. *****\n";
  private static final String TEST_PASSED_MESSAGE = "***** Integration test passed. *****\n";

  /** Converts input string USER_MODE to enum Mode.*/
  public static class ModeConverter implements IStringConverter<Mode> {
    @Override
    public Mode convert(String value) {
      switch (value) {
        case "dfs":
          return Mode.DEFAULT_FILESYSTEM;
        case "location":
          return Mode.LOCATION;
        default:
          throw new ParameterException("-mode USER_MODE, USER_MODE is dfs or location, "
                  + "your USER_MODE is invalid");
      }
    }
  }

  /** Hive and Alluxio integration mode.*/
  public enum Mode {
    DEFAULT_FILESYSTEM, // Alluxio is configured as Hive default filesystem
    LOCATION; // Alluxio is used as a location of Hive tables other than Hive default filesystem
  }

  @Parameter(names = {"-alluxioUrl"}, description = "the alluxio cluster url in the form "
      + "alluxio://master_hostname:port")
  private String mAlluxioURL = "";

  @Parameter(names = {"-mode"}, description = "dfs means Alluxio is configured as "
      + "Hive default filesystem, location means Alluxio is used as "
      + "a location of Hive tables other than Hive default filesystem.",
      converter = ModeConverter.class)
  private Mode mUserMode = Mode.LOCATION;

  @Parameter(names = {"-hiveUrl", "-hiveurl"}, description = "a Hive connection url "
      + "in the form jdbc:subprotocol:subname", required = true)
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
      String tableName = "AlluxioTestTable";
      String sql = String.format("drop table if exists %s", tableName);
      try (PreparedStatement dropTablePS = con.prepareStatement(sql)) {
        dropTablePS.execute();
      }

      // Create test table based on different integration ways
      if (mUserMode.equals(Mode.DEFAULT_FILESYSTEM)) {
        createTableInHiveDFS(con, tableName);
      } else {
        createTableInAlluxio(con, tableName);
      }

      sql = String.format("INSERT INTO %s VALUES ('You passed ', 'Hive Test!')", tableName);
      try (PreparedStatement loadTablePS = con.prepareStatement(sql)) {
        loadTablePS.executeUpdate();
      }

      sql = String.format("describe %s", tableName);
      try (PreparedStatement describeTablePS = con.prepareStatement(sql)) {
        describeTablePS.execute();
      }

      sql = String.format("select * from %s", tableName);
      reportWriter.println("Running " + sql);
      try (PreparedStatement selectTablePS = con.prepareStatement(sql);
           ResultSet resultSet = selectTablePS.executeQuery()) {
        reportWriter.println("Result should be \"You passed Hive test!\" ");
        reportWriter.println("Checker result is: ");
        while (resultSet.next()) {
          reportWriter.println(resultSet.getString(1) + resultSet.getString(2));
        }
      }
    } catch (Exception e) {
      printExceptionReport(e, reportWriter);
      return 1;
    }
    reportWriter.println("\nCongratulations, you have configured Hive with Alluxio correctly!\n");
    reportWriter.println(TEST_PASSED_MESSAGE);
    return 0;
  }

  /**
   * Creates a test table stored in Alluxio filesystem (Hive default filesystem).
   *
   * @param con the Hive connection
   * @param tableName the name of the test table
   */
  private void createTableInHiveDFS(Connection con, String tableName) throws Exception {
    String sql = String.format("CREATE TABLE %s (ROW1 STRING, ROW2 STRING) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
        + "STORED AS TEXTFILE", tableName);
    try (PreparedStatement createTablePS = con.prepareStatement(sql)) {
      createTablePS.execute();
    }
  }

  /**
   * Creates a test table stored in Alluxio filesystem (not Hive default filesystem).
   *
   * @param con the Hive connection
   * @param tableName the name of the test table
   */
  private void createTableInAlluxio(Connection con, String tableName) throws Exception {
    String sql = String.format("CREATE TABLE %s (ROW1 STRING, ROW2 STRING) "
        + "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' "
        + "LOCATION '%s/alluxioTestFolder/'", tableName, mAlluxioURL);
    try (PreparedStatement createTablePS = con.prepareStatement(sql)) {
      createTablePS.execute();
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
    if (exceptionStr.contains("Could not open client transport with JDBC Uri")) {
      exception.printStackTrace(reportWriter);
      reportWriter.printf("Could not open client transport with JDBC Uri: %s%n", mHiveURL);
      reportWriter.println("Please modify your Hive Url and rerun the checker.");
    } else if (exceptionStr.contains("Class alluxio.hadoop.FileSystem not found")) {
      reportWriter.println(FAIL_TO_FIND_CLASS_MESSAGE);
    } else if (exceptionStr.contains("No FileSystem for scheme \"alluxio\"")
        || exceptionStr.contains("No FileSystem for scheme: alluxio")) {
      reportWriter.println(FAIL_TO_FIND_FS_MESSAGE);
    } else {
      reportWriter.println("Please fix the following error and "
          + "rerun the Hive Integration Checker.\n");
      exception.printStackTrace(reportWriter);
    }
    reportWriter.println(TEST_FAILED_MESSAGE);
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

    try (PrintWriter reportWriter = CheckerUtils.initReportFile()) {
      int result = checker.run(reportWriter);
      reportWriter.flush();
      System.exit(result);
    }
  }
}
