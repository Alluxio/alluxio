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

package alluxio.testutils;

import alluxio.AlluxioURI;
import alluxio.AuthenticatedClientUserResource;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemCrossCluster;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.DeletePOptions;
import alluxio.master.LocalAlluxioCluster;
import alluxio.master.file.FileSystemMaster;
import alluxio.master.file.contexts.DeleteContext;
import alluxio.master.file.contexts.ListStatusContext;
import alluxio.metrics.MetricsSystem;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.underfs.UfsMode;
import alluxio.util.SecurityUtils;
import alluxio.wire.FileInfo;

import org.apache.ratis.util.Preconditions;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A JUnit Rule resource for automatically managing a local alluxio cluster for testing. To use it,
 * create an instance of the class under a {@literal @}Rule annotation, with the required
 * configuration parameters, and any necessary explicit {@link Configuration} settings. The
 * Alluxio cluster will be set up from scratch at the end of every method (or at the start of
 * every suite if {@literal @}ClassRule is used), and destroyed at the end. Below is an example
 * of declaring and using it.
 *
 * <pre>
 *   public class SomethingTest {
 *    {@literal @}Rule
 *    public LocalAlluxioClusterResource localAlluxioClusterResource =
 *      new LocalAlluxioClusterResource(WORKER_CAPACITY, BLOCK_SIZE);
 *
 *    {@literal @}Test
 *    public void testSomething() {
 *      localAlluxioClusterResource.get().getClient().create("/abced");
 *      ...
 *    }
 *
 *    {@literal @}Test
 *    {@literal @}LocalAlluxioClusterResource.Config(
 *        confParams = {CONF_KEY_1, CONF_VALUE_1, CONF_KEY_2, CONF_VALUE_2, ...})
 *    public void testSomethingWithDifferentConf() {
  *      localAlluxioClusterResource.get().getClient().create("/efghi");
 *      ...
 *    }
 *
 *    {@literal @}Test
 *    {@literal @}LocalAlluxioClusterResource.Config(startCluster = false)
 *    public void testSomethingWithClusterStartedManually() {
 *      localAlluxioClusterResource.start();
 *      localAlluxioClusterResource.get().getClient().create("/efghi");
 *      ...
 *    }
 *   }
 * </pre>
 */
@NotThreadSafe
public final class LocalAlluxioClusterResource implements TestRule {

  /** Number of Alluxio workers in the cluster. */
  private final int mNumWorkers;

  /** Weather to include the secondary master. */
  private final boolean mIncludeSecondary;

  /** Weather to include the proxy. */
  private final boolean mIncludeProxy;

  /**
   * If true (default), we start the cluster before running a test method. Otherwise, the method
   * must start the cluster explicitly.
   */
  private final boolean mStartCluster;

  /**
   * If true, the cluster will also start a cross cluster master standalone.
   */
  private boolean mStartCrossClusterStandalone = false;

  /** Configuration values for the cluster. */
  private final Map<PropertyKey, Object> mConfiguration = new HashMap<>();

  /** The Alluxio cluster being managed. */
  private LocalAlluxioCluster mLocalAlluxioCluster = null;

  /** The name of the test/cluster. */
  private String mTestName = "test";

  private AlluxioConfiguration mSetConfig;

  /**
   * Creates a new instance.
   *
   * @param startCluster whether to start the cluster before the test method starts
   * @param numWorkers the number of Alluxio workers to launch
   * @param configuration configuration for configuring the cluster
   */
  private LocalAlluxioClusterResource(boolean startCluster, boolean includeSecondary,
      boolean includeProxy, boolean startCrossClusterStandalone, int numWorkers,
      Map<PropertyKey, Object> configuration) {
    mStartCluster = startCluster;
    mIncludeSecondary = includeSecondary;
    mIncludeProxy = includeProxy;
    mStartCrossClusterStandalone = startCrossClusterStandalone;
    mNumWorkers = numWorkers;
    mConfiguration.putAll(configuration);
    if (!mConfiguration.containsKey(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE)
        && !mConfiguration.containsKey(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE)) {
      mConfiguration.put(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE, 2);
      mConfiguration.put(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE, 2);
    }
    mConfiguration.putIfAbsent(PropertyKey.USER_NETWORK_RPC_NETTY_WORKER_THREADS, 2);
    mConfiguration.putIfAbsent(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE, 1000);
    MetricsSystem.resetCountersAndGauges();
  }

  /**
   * @return the {@link LocalAlluxioCluster} being managed
   */
  public LocalAlluxioCluster get() {
    return mLocalAlluxioCluster;
  }

  /**
   * @return a {@link FileSystemCrossCluster} client
   */
  public FileSystemCrossCluster getCrossClusterClient() {
    return get().getCrossClusterClient(FileSystemContext.create(mSetConfig));
  }

  /**
   * Adds a property to the cluster resource. Unset a property by passing a null value.
   *
   * @param key property key
   * @param value property value
   * @return the cluster resource
   */
  public LocalAlluxioClusterResource setProperty(PropertyKey key, Object value) {
    if (value == null) {
      mConfiguration.remove(key);
    } else {
      mConfiguration.put(key, value);
    }
    return this;
  }

  /**
   * Explicitly starts the {@link LocalAlluxioCluster}.
   */
  public void start() throws Exception {
    AuthenticatedClientUser.remove();
    // Create a new cluster.
    mLocalAlluxioCluster = new LocalAlluxioCluster(mNumWorkers, mIncludeSecondary, mIncludeProxy,
        mStartCrossClusterStandalone);
    // Init configuration for integration test
    mLocalAlluxioCluster.initConfiguration(mTestName);
    // Overwrite the test configuration with test specific parameters
    for (Entry<PropertyKey, Object> entry : mConfiguration.entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
    Configuration.global().validate();
    // Start the cluster
    mLocalAlluxioCluster.start();
    mSetConfig = Configuration.copyGlobal();
  }

  /**
   * Explicitly stops the {@link LocalAlluxioCluster}.
   */
  public void stop() throws Exception {
    mLocalAlluxioCluster.stop();
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        IntegrationTestUtils.reserveMasterPorts();
        mTestName = IntegrationTestUtils
            .getTestName(description.getTestClass().getSimpleName(), description.getMethodName());
        try {
          try {
            Annotation configAnnotation;
            configAnnotation = description.getAnnotation(ServerConfig.class);
            if (configAnnotation != null) {
              overrideConfiguration(((ServerConfig) configAnnotation).confParams());
            }

            boolean startCluster = mStartCluster;
            configAnnotation = description.getAnnotation(Config.class);
            if (configAnnotation != null) {
              Config config = (Config) configAnnotation;
              overrideConfiguration(config.confParams());
              // Override startCluster
              startCluster = config.startCluster();
            }
            if (startCluster) {
              start();
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          try {
            statement.evaluate();
          } finally {
            stop();
          }
        } finally {
          IntegrationTestUtils.releaseMasterPorts();
        }
      }
    };
  }

  /**
   * @param config the array of strings for the configuration values to override
   */
  private void overrideConfiguration(String[] config) {
    // Override the configuration parameters with any configuration params
    for (int i = 0; i < config.length; i += 2) {
      PropertyKey key = PropertyKey.fromString(config[i]);
      mConfiguration.put(key, key.parseValue(config[i + 1]));
    }
  }

  /**
   * Returns a resource which will reset the cluster without restarting it. The rule will perform
   * operations on the running cluster to get back to a clean state. This is primarily useful for
   * when the {@link LocalAlluxioCluster} is a {@code @ClassRule}, so this reset rule can reset
   * the cluster between tests.
   *
   * @return a {@link TestRule} for resetting the cluster
   */
  public TestRule getResetResource() {
    return new ResetRule(this);
  }

  /**
   * Builder for a {@link LocalAlluxioClusterResource}.
   */
  public static class Builder {
    private boolean mStartCluster;
    private boolean mIncludeSecondary;
    private boolean mIncludeProxy;
    private int mNumWorkers;
    private boolean mStartCrossClusterStandalone = false;
    private Map<PropertyKey, Object> mConfiguration;

    /**
     * Constructs the builder with default values.
     */
    public Builder() {
      mStartCluster = true;
      mIncludeSecondary = false;
      mIncludeProxy = false;
      mNumWorkers = 1;
      mConfiguration = new HashMap<>();
    }

    /**
     * @param startCluster whether to start the cluster at the start of the test
     */
    public Builder setStartCluster(boolean startCluster) {
      mStartCluster = startCluster;
      return this;
    }

    /**
     * When this is called, a cross cluster standalone process will also be started with
     * the cluster.
     */
    public Builder includeCrossClusterStandalone() {
      boolean enableCrossCluster = Optional.ofNullable((Boolean) mConfiguration.get(
              PropertyKey.MASTER_CROSS_CLUSTER_ENABLE))
          .orElse(Configuration.getBoolean(PropertyKey.MASTER_CROSS_CLUSTER_ENABLE));
      boolean crossClusterStandAlone = Optional.ofNullable((Boolean) mConfiguration.get(
              PropertyKey.CROSS_CLUSTER_MASTER_STANDALONE))
          .orElse(Configuration.getBoolean(PropertyKey.CROSS_CLUSTER_MASTER_STANDALONE));
      Preconditions.assertTrue(enableCrossCluster && crossClusterStandAlone,
          "%s and %s must be enabled to start a cross cluster standalone",
          PropertyKey.MASTER_CROSS_CLUSTER_ENABLE,
          PropertyKey.CROSS_CLUSTER_MASTER_STANDALONE);
      mStartCrossClusterStandalone = true;
      return this;
    }

    /**
     * @param includeSecondary whether to include the secondary master
     */
    public Builder setIncludeSecondary(boolean includeSecondary) {
      mIncludeSecondary = includeSecondary;
      return this;
    }

    /**
     * @param includeProxy whether to include the proxy
     */
    public Builder setIncludeProxy(boolean includeProxy) {
      mIncludeProxy = includeProxy;
      return this;
    }

    /**
     * @param numWorkers the number of workers to run in the cluster
     */
    public Builder setNumWorkers(int numWorkers) {
      mNumWorkers = numWorkers;
      return this;
    }

    /**
     * @param key the property key to set for the cluster
     * @param value the value to set it to
     */
    public Builder setProperty(PropertyKey key, Object value) {
      mConfiguration.put(key, value);
      return this;
    }

    /**
     * @return a {@link LocalAlluxioClusterResource} for the current builder values
     */
    public LocalAlluxioClusterResource build() {
      return new LocalAlluxioClusterResource(mStartCluster, mIncludeSecondary, mIncludeProxy,
          mStartCrossClusterStandalone, mNumWorkers, mConfiguration);
    }
  }

  private final class ResetRule implements TestRule {
    private final LocalAlluxioClusterResource mCluster;

    ResetRule(LocalAlluxioClusterResource cluster) {
      mCluster = cluster;
    }

    @Override
    public Statement apply(Statement statement, Description description) {
      return new Statement() {
        @Override
        public void evaluate() throws Throwable {
          try {
            statement.evaluate();
          } finally {
            FileSystemMaster fsm =
                mCluster.mLocalAlluxioCluster.getLocalAlluxioMaster().getMasterProcess()
                    .getMaster(FileSystemMaster.class);

            if (SecurityUtils.isAuthenticationEnabled(Configuration.global())) {
              // Reset the state as the root inode user (superuser).
              try (AuthenticatedClientUserResource r = new AuthenticatedClientUserResource(
                  fsm.getRootInodeOwner(), Configuration.global())) {
                resetCluster(fsm);
              }
            } else {
              resetCluster(fsm);
            }
          }
        }
      };
    }

    private void resetCluster(FileSystemMaster fsm) throws Exception {
      if (!mCluster.get().getLocalAlluxioMaster().isServing()) {
        // Restart the masters if they are not serving.
        mCluster.mLocalAlluxioCluster.startMasters();
        // use the newly created/started file system master
        fsm = mCluster.mLocalAlluxioCluster.getLocalAlluxioMaster().getMasterProcess()
            .getMaster(FileSystemMaster.class);
      }
      if (!mCluster.get().isStartedWorkers()) {
        // Start the new workers
        mCluster.get().startWorkers();
      }
      fsm.updateUfsMode(new AlluxioURI(fsm.getUfsAddress()), UfsMode.READ_WRITE);
      for (FileInfo fileInfo : fsm
          .listStatus(new AlluxioURI("/"), ListStatusContext.defaults())) {
        fsm.delete(new AlluxioURI(fileInfo.getPath()), DeleteContext
            .create(DeletePOptions.newBuilder()
                .setUnchecked(true)
                .setRecursive(true)
                .setDeleteMountPoint(true)));
      }
    }
  }

  /**
   * An annotation for test methods that can be used to override any of the defaults set at
   * construction time.
   */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Config {
    String[] confParams() default {};
    boolean startCluster() default true;
  }

  /**
   * Class-level annotations that can override the configuration for static class rules.
   */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ServerConfig {
    String[] confParams() default {};
  }
}
