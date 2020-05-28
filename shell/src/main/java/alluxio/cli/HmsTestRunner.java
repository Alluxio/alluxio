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

package alluxio.cli;

import alluxio.cli.RunTestUtils.State;
import alluxio.cli.RunTestUtils.TaskResult;
import alluxio.util.CommonUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Run tests against an existing hive metastore.
 */
public class HmsTestRunner {
  // The maximum number of table objects that this test will get.
  // Used to avoid issuing too many calls to the hive metastore
  // which may need a long time based on network conditions
  private static final int GET_TABLE_OBJECT_THRESHOLD = 5;

  @Parameter(names = {"-h", "--help"}, help = true)
  private boolean mHelp = false;

  @Parameter(names = {"-m", "--metastoreUri"}, required = true,
      description = "Uri(s) to connect to hive metastore.")
  private String mMetastoreUri;

  @Parameter(names = {"-d", "--database"}, required = false,
      description = "Database to run tests against.")
  private String mDatabase = "default";

  @Parameter(names = {"-t", "--tables"}, required = false,
      description = "Database to run tests against.")
  private String mTables;

  @Parameter(names = {"-st", "--socketTimeout"}, required = false,
      description = "Socket timeout of hive metastore client.")
  private int mSocketTimeout = 12;

  private IMetaStoreClient mClient;

  private Map<State, List<TaskResult>> mResults = new HashMap<>();

  /**
   * Runs tests against an existing hive metastore.
   * If an error occur, all the following tests will be skipped.
   *
   * @return 0 if succeed, -1 if error occurs
   */
  private int run() {
    try {
      checkConfiguration();
      HiveConf hiveConf = setHiveConf();
      mClient = createHiveMetastoreClient(hiveConf);
      try {
        getDatabaseTest();
        if (mTables != null) {
          getTableObjectsTest(Arrays.asList(mTables.split(",")));
        } else {
          getAllTableInfoTest();
        }
      } finally {
        mClient.close();
      }
    } catch (Throwable t) {
      printCheckReport(t);
      return -1;
    }
    printCheckReport(null);
    return 0;
  }

  /**
   * Check if the given configuration is valid.
   *
   * @throws Exception if any of the given configuration is invalid
   */
  private void checkConfiguration() throws Exception {
    if (mMetastoreUri.contains(",")) {
      // In HA mode, all the uris format need to be valid,
      // but only one of the uris needs to be reachable.
      String[] uris = mMetastoreUri.split(",");
      boolean uriReachable = false;
      for (String uri : uris) {
        if (checkHmsUri(uri)) {
          uriReachable = true;
        }
      }
      if (!uriReachable) {
        String errorMessage = "Hive metastore uris are unreachable";
        mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
            new TaskResult(State.FAILED, "HmsUrisReachableCheck", errorMessage,
                "Please make sure the given hive metastore uris are reachable"));
        throw new IOException(errorMessage);
      }
    } else {
      checkHmsUri(mMetastoreUri);
    }
  }

  /**
   * Checks if the given uri is a valid and reachable hive metastore uri.
   *
   * @param  uriAddress the uri address
   * @return true if the address is reachable, false otherwise
   * @throws Exception if the given uri is invalid
   */
  private boolean checkHmsUri(String uriAddress) throws Exception {
    URI uri;
    try {
      uri = new URI(uriAddress);
    } catch (Throwable t) {
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
          new TaskResult(State.FAILED, "HmsUrisSyntaxCheck", getErrorInfo(t),
              "Please make sure the given hive metastore uri(s) is valid"));
      throw t;
    }

    if (uri.getHost() == null || uri.getPort() == -1 || !uri.getScheme().equals("thrift")) {
      String errorMessage = "Invalid hive metastore uris";
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
          new TaskResult(State.FAILED, "HmsUrisSyntaxCheck", errorMessage,
              "Please make sure the given hive metastore uri(s) is valid"));
      throw new IOException(errorMessage);
    }

    try {
      InetAddress.getByName(uri.getHost());
    } catch (Throwable t) {
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
          new TaskResult(State.FAILED, "HmsUrisHostnameResolvableCheck", getErrorInfo(t),
              "Please make sure the hostname in given hive metastore uri(s) is resolvable"));
      throw t;
    }

    // Note that the port may be reachable but not the actual HMS port
    return CommonUtils.isAddressReachable(uri.getHost(), uri.getPort());
  }

  /**
   * Sets hive configuration based on input parameters.
   *
   * @return the hive configuration
   */
  private HiveConf setHiveConf() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, mMetastoreUri);
    conf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, mSocketTimeout);
    return conf;
  }

  /**
   * Create hive metastore client.
   *
   * @param hiveConf the hive configuration for this client
   * @return the created hive metastore client
   * @throws Exception if failed to create hive metastore client
   */
  private IMetaStoreClient createHiveMetastoreClient(HiveConf hiveConf) throws Exception {
    ConnectHmsAction action;
    String testName = "CreateHmsClientTest";
    try {
      action = new ConnectHmsAction(hiveConf);
      UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
      ugi.doAs(action);
    } catch (UndeclaredThrowableException e) {
      if (e.getUndeclaredThrowable() instanceof IMetaStoreClient.IncompatibleMetastoreException) {
        mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
            new TaskResult(State.FAILED, testName, getErrorInfo(e),
                String.format("Hive metastore client (version: %s) is incompatible with "
                        + "your Hive Metastore server version",
                    IMetaStoreClient.class.getPackage().getImplementationVersion())));
      } else {
        mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
            new TaskResult(State.FAILED, testName, getErrorInfo(e),
                "Failed to create hive metastore client. "
                    + "Please check if the given hive metastore uris is valid and reachable"));
      }
      throw e;
    } catch (InterruptedException e) {
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
          new TaskResult(State.FAILED, testName, getErrorInfo(e),
              "Hive metastore client creation is interrupted. Please rerun the test if needed"));
      throw e;
    } catch (Throwable t) {
      String errorInfo = getErrorInfo(t);
      TaskResult result = new TaskResult()
          .setState(State.FAILED).setName(testName).setOutput(errorInfo);
      if (errorInfo.contains("Could not connect to meta store using any of the URIs provided")) {
        // Happens when the hms port is reachable but not the actual hms port
        // Thrown as RuntimeException with this error message
        result.setAdvice("Failed to create hive metastore client. "
            + "Please check if the given hive metastore uri(s) is valid and reachable");
      } else {
        result.setAdvice("Failed to create hive metastore client");
      }
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(result);
      throw t;
    }
    return action.getConnection();
  }

  private void getDatabaseTest() throws Exception {
    String testName = "GetDatabase";
    try {
      Database database = mClient.getDatabase(mDatabase);
      mResults.computeIfAbsent(State.OK, k -> new ArrayList<>()).add(
          new TaskResult(State.OK, testName, String.format("Database (name: %s, description: %s)",
              database.getName(), database.getDescription()), ""));
    } catch (NoSuchObjectException e) {
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
          new TaskResult(State.FAILED, testName, getErrorInfo(e),
              "Please make sure the given database name is valid "
                  + "and existing in the target hive metastore"));
      throw e;
    } catch (Throwable t) {
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>()).add(
          new TaskResult(State.FAILED, testName, getErrorInfo(t),
              "Failed to get database from remote hive metastore"));
      throw t;
    }
  }

  private void getAllTableInfoTest() {
    List<String> tables;
    String testName = "GetAllTables";
    try {
      tables = mClient.getAllTables(mDatabase);
    } catch (Throwable t) {
      addThrowableWarning(testName, t, "Database: " + mDatabase);
      return;
    }
    mResults.computeIfAbsent(State.OK, k -> new ArrayList<>()).add(
        new TaskResult(State.OK, testName,
            String.format("Database (name: %s, tables: %s)", mDatabase, tables.toString()), ""));
    if (tables.size() == 0) {
      return;
    }
    if (tables.size() > GET_TABLE_OBJECT_THRESHOLD) {
      tables = tables.subList(0, GET_TABLE_OBJECT_THRESHOLD);
    }
    getTableObjectsTest(tables);
  }

  private void getTableObjectsTest(List<String> tableNames) {
    String opTarget = "tables: " + String.join(",", tableNames);
    String testName = "GetTableObjectsByName";
    try {
      // This call may take too long if the database contains hundred thousands of tables
      // and the network between hms and client is really bad
      List<Table> tables = mClient.getTableObjectsByName(mDatabase, tableNames);
      StringBuilder tableLocations = new StringBuilder();
      for (Table table : tables) {
        tableLocations.append(String.format("Table (name: %s, location: %s)\n",
            table.getTableName(), table.getSd().getLocation()));
      }
      mResults.computeIfAbsent(State.OK, k -> new ArrayList<>()).add(
          new TaskResult(State.OK, testName, tableLocations.toString(), ""));
    } catch (Throwable t) {
      addThrowableWarning(testName, t, opTarget);
    }

    testName = "GetTableSchema";
    try {
      StringBuilder tableFieldsOutput = new StringBuilder();
      for (String table : tableNames) {
        String tableFields = mClient.getSchema(mDatabase, table).stream()
            .map(FieldSchema::getName).collect(Collectors.joining(","));
        tableFieldsOutput.append(String
            .format("Table (name: %s, fields: %s)\n", table, tableFields));
      }
      mResults.computeIfAbsent(State.OK, k -> new ArrayList<>()).add(
          new TaskResult(State.OK, testName, tableFieldsOutput.toString(), ""));
    } catch (Throwable t) {
      addThrowableWarning(testName, t, opTarget);
    }
  }

  private String getErrorInfo(Throwable t) {
    StringWriter errors = new StringWriter();
    t.printStackTrace(new PrintWriter(errors));
    return errors.toString();
  }

  private void addThrowableWarning(String opName, Throwable t, String opTarget) {
    TaskResult taskResult = new TaskResult().setState(State.WARNING).setName(opName)
        .setOutput(getErrorInfo(t));
    if (t instanceof InvalidOperationException) {
      taskResult.setAdvice(opName + " is invalid");
    } else if (t instanceof UnknownDBException) {
      taskResult.setAdvice("Please make sure the given database name is valid "
          + "and existing in the target hive metastore");
    } else if (t instanceof UnknownTableException) {
      taskResult.setAdvice("Please make sure the given table names are valid "
          + "and existing in the target hive metastore");
    } else {
      taskResult.setAdvice(String.format("Failed to run %s (%s)", opName, opTarget));
    }
    mResults.computeIfAbsent(State.WARNING, k -> new ArrayList<>()).add(taskResult);
  }

  private void printCheckReport(Throwable throwable) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    if (throwable != null && mResults.get(State.FAILED) == null) {
      // Should not reach here!
      mResults.computeIfAbsent(State.FAILED, k -> new ArrayList<>())
          .add(new TaskResult(State.FAILED, "UnexpectedError", getErrorInfo(throwable),
              "Failed to run hive metastore tests"));
    }
    System.out.println(gson.toJson(mResults));
  }

  /**
   * @param args the input arguments
   */
  public static void main(String[] args) throws Exception {
    HmsTestRunner test = new HmsTestRunner();
    JCommander jc = new JCommander(test, args);
    if (test.mHelp) {
      jc.usage();
      return;
    }
    System.exit(test.run());
  }

  /**
   * An action to connect to remote Hive metastore.
   */
  static class ConnectHmsAction implements java.security.PrivilegedExceptionAction<Void> {
    private IMetaStoreClient mConnection;
    private HiveConf mHiveConf;

    public ConnectHmsAction(HiveConf conf) {
      mHiveConf = conf;
    }

    public IMetaStoreClient getConnection() {
      return mConnection;
    }

    @Override
    public Void run() throws MetaException {
      IMetaStoreClient client = RetryingMetaStoreClient
          .getProxy(mHiveConf, table -> null, HiveMetaStoreClient.class.getName());
      mConnection = client;
      return null;
    }
  }
}
