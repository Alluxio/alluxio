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

package alluxio.cli.hms;

import alluxio.cli.ValidationTaskResult;
import alluxio.cli.ValidationUtils;
import alluxio.collections.Pair;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;

import java.lang.reflect.UndeclaredThrowableException;
import java.util.Map;

/**
 * A validation task that generates a new Hive Metastore client and verifies the connection.
 */
public class CreateHmsClientValidationTask extends
    MetastoreValidationTask<String, IMetaStoreClient> {

  private final int mSocketTimeoutSeconds;

  /**
   * Create a new instance of {@link CreateHmsClientValidationTask}.
   *
   * @param socketTimeout the timeout when connecting to the metastore
   * @param input the input task providing the URI(s)
   */
  public CreateHmsClientValidationTask(int socketTimeout,
      MetastoreValidationTask<?, String> input) {
    super(input);
    mSocketTimeoutSeconds = socketTimeout;
  }

  @Override
  public String getName() {
    return "CreateHmsClientTest";
  }

  @Override
  public ValidationTaskResult validateImpl(Map<String, String> optionMap)
      throws InterruptedException {
    Pair<ValidationTaskResult, IMetaStoreClient> ent = getValidationWithResult();
    if (ent.getFirst() == null) {
      ent.getSecond().close();
      return new ValidationTaskResult(ValidationUtils.State.OK, getName(),
          "Metastore connection successful", "");
    }
    return ent.getFirst();
  }

  /**
   * @return the result of the validation. {@link Pair#getFirst()} will be null if the connection
   * to the metastore was successful, otherwise if there is a failure {@link Pair#getSecond()}
   * will be null
   * @throws InterruptedException if the task is interrupted
   */
  protected Pair<ValidationTaskResult, IMetaStoreClient> getValidationWithResult()
      throws InterruptedException {
    if (mInputTask == null) {
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
          "pre-requisite check on metastore URI failed.", ""), null);
    }
    Pair<ValidationTaskResult, String> inputResult =
        mInputTask.getValidationWithResult();
    if (inputResult.getFirst().getState() != ValidationUtils.State.OK) {
      return new Pair<>(inputResult.getFirst(), null);
    }
    String metastoreUri = inputResult.getSecond();
    try {
      ConnectHmsAction action;
      ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();
      try {
        // Use the extension class loader
        Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
        HiveConf conf = new HiveConf();
        conf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreUri);
        conf.setIntVar(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, mSocketTimeoutSeconds);
        action = new ConnectHmsAction(conf);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        ugi.doAs(action);
      } finally {
        Thread.currentThread().setContextClassLoader(currentClassLoader);
      }
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.OK, getName(),
          "Metastore connection successful", ""),
          action.getConnection());
    } catch (UndeclaredThrowableException e) {
      if (e.getUndeclaredThrowable() instanceof IMetaStoreClient.IncompatibleMetastoreException) {
        return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
            ValidationUtils.getErrorInfo(e),
            String.format("Hive metastore client (version: %s) is incompatible with "
                    + "your Hive Metastore server version",
                IMetaStoreClient.class.getPackage().getImplementationVersion())), null);
      } else {
        return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
            ValidationUtils.getErrorInfo(e),
            "Failed to create hive metastore client. "
                + "Please check if the given hive metastore uris is valid and reachable"), null);
      }
    } catch (InterruptedException e) {
      return new Pair<>(new ValidationTaskResult(ValidationUtils.State.FAILED, getName(),
          ValidationUtils.getErrorInfo(e),
          "Hive metastore client creation is interrupted. Please rerun the test if needed"), null);
    } catch (Throwable t) {
      String errorInfo = ValidationUtils.getErrorInfo(t);
      ValidationTaskResult result = new ValidationTaskResult()
          .setState(ValidationUtils.State.FAILED).setName(getName()).setOutput(errorInfo);
      if (errorInfo.contains("Could not connect to meta store using any of the URIs provided")) {
        // Happens when the hms port is reachable but not the actual hms port
        // Thrown as RuntimeException with this error message
        result.setAdvice("Failed to create hive metastore client. "
            + "Please check if the given hive metastore uri(s) is valid and reachable");
      } else {
        result.setAdvice("Failed to create hive metastore client");
      }
      return new Pair<>(result, null);
    }
  }

  /**
   * An action to connect to remote Hive metastore.
   */
  static class ConnectHmsAction implements java.security.PrivilegedExceptionAction<Void> {
    private IMetaStoreClient mConnection;
    private final HiveConf mHiveConf;

    public ConnectHmsAction(HiveConf conf) {
      mHiveConf = conf;
    }

    public IMetaStoreClient getConnection() {
      return mConnection;
    }

    @Override
    public Void run() throws MetaException {
      mConnection = RetryingMetaStoreClient
          .getProxy(mHiveConf, table -> null, HiveMetaStoreClient.class.getName());
      return null;
    }
  }
}
