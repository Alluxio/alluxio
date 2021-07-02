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
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A JUnit Rule resource for automatically managing a local alluxio cluster for testing. To use it,
 * create an instance of the class under a {@literal @}Rule annotation, with the required
 * configuration parameters, and any necessary explicit {@link ServerConfiguration} settings. The
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

  /**
   * If true (default), we start the cluster before running a test method. Otherwise, the method
   * must start the cluster explicitly.
   */
  private final boolean mStartCluster;

  /** ServerConfiguration values for the cluster. */
  private final Map<PropertyKey, String> mConfiguration = new HashMap<>();

  /** The Alluxio cluster being managed. */
  private LocalAlluxioCluster mLocalAlluxioCluster = null;

  /** The name of the test/cluster. */
  private String mTestName = "test";

  /**
   * Creates a new instance.
   *
   * @param startCluster whether or not to start the cluster before the test method starts
   * @param numWorkers the number of Alluxio workers to launch
   * @param configuration configuration for configuring the cluster
   */
  private LocalAlluxioClusterResource(boolean startCluster, boolean includeSecondary,
      int numWorkers, Map<PropertyKey, String> configuration) {
    mStartCluster = startCluster;
    mIncludeSecondary = includeSecondary;
    mNumWorkers = numWorkers;
    mConfiguration.putAll(configuration);
    MetricsSystem.resetCountersAndGauges();
  }

  /**
   * @return the {@link LocalAlluxioCluster} being managed
   */
  public LocalAlluxioCluster get() {
    return mLocalAlluxioCluster;
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
      mConfiguration.put(key, value.toString());
    }
    return this;
  }

  /**
   * Explicitly starts the {@link LocalAlluxioCluster}.
   */
  public void start() throws Exception {
    AuthenticatedClientUser.remove();
    // Create a new cluster.
    mLocalAlluxioCluster = new LocalAlluxioCluster(mNumWorkers, mIncludeSecondary);
    // Init configuration for integration test
    mLocalAlluxioCluster.initConfiguration(mTestName);
    // Overwrite the test configuration with test specific parameters
    for (Entry<PropertyKey, String> entry : mConfiguration.entrySet()) {
      ServerConfiguration.set(entry.getKey(), entry.getValue());
    }
    ServerConfiguration.global().validate();
    // Start the cluster
    mLocalAlluxioCluster.start();
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
      mConfiguration.put(PropertyKey.fromString(config[i]), config[i + 1]);
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
    private int mNumWorkers;
    private Map<PropertyKey, String> mConfiguration;

    /**
     * Constructs the builder with default values.
     */
    public Builder() {
      mStartCluster = true;
      mIncludeSecondary = false;
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
     * @param includeSecondary whether to include the secondary master
     */
    public Builder setIncludeSecondary(boolean includeSecondary) {
      mIncludeSecondary = includeSecondary;
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
      mConfiguration.put(key, value.toString());
      return this;
    }

    /**
     * @return a {@link LocalAlluxioClusterResource} for the current builder values
     */
    public LocalAlluxioClusterResource build() {
      return new LocalAlluxioClusterResource(mStartCluster, mIncludeSecondary, mNumWorkers,
          mConfiguration);
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

            if (SecurityUtils.isAuthenticationEnabled(ServerConfiguration.global())) {
              // Reset the state as the root inode user (superuser).
              try (AuthenticatedClientUserResource r = new AuthenticatedClientUserResource(
                  fsm.getRootInodeOwner(), ServerConfiguration.global())) {
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
            .create(DeletePOptions.newBuilder().setUnchecked(true).setRecursive(true)));
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
