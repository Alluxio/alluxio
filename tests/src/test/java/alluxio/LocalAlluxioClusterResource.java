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

package alluxio;

import alluxio.master.LocalAlluxioCluster;
import alluxio.metrics.MetricsSystem;
import alluxio.security.LoginUserTestUtils;
import alluxio.security.authentication.AuthenticatedClientUser;

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
 * configuration parameters, and any necessary explicit {@link Configuration} settings. The Alluxio
 * cluster will be set up from scratch at the end of every method (or at the start of every suite if
 * {@literal @}ClassRule is used), and destroyed at the end. Below is an example of declaring and
 * using it.
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

  /**
   * If true (default), we start the cluster before running a test method. Otherwise, the method
   * must start the cluster explicitly.
   */
  private final boolean mStartCluster;

  /** Configuration values for the cluster. */
  private final Map<PropertyKey, String> mConfiguration = new HashMap<>();

  /** The Alluxio cluster being managed. */
  private LocalAlluxioCluster mLocalAlluxioCluster = null;

  /**
   * Creates a new instance.
   *
   * @param startCluster whether or not to start the cluster before the test method starts
   * @param numWorkers the number of Alluxio workers to launch
   * @param configuration configuration for configuring the cluster
   */
  private LocalAlluxioClusterResource(boolean startCluster, int numWorkers,
      Map<PropertyKey, String> configuration) {
    mStartCluster = startCluster;
    mNumWorkers = numWorkers;
    mConfiguration.putAll(configuration);
    MetricsSystem.resetAllCounters();
  }

  /**
   * @return the {@link LocalAlluxioCluster} being managed
   */
  public LocalAlluxioCluster get() {
    return mLocalAlluxioCluster;
  }

  /**
   * Adds a property to the cluster resource.
   *
   * @param key property key
   * @param value property value
   * @return the cluster resource
   */
  public LocalAlluxioClusterResource setProperty(PropertyKey key, Object value) {
    mConfiguration.put(key, value.toString());
    return this;
  }

  /**
   * Explicitly starts the {@link LocalAlluxioCluster}.
   */
  public void start() throws Exception {
    AuthenticatedClientUser.remove();
    LoginUserTestUtils.resetLoginUser();
    // Create a new cluster.
    mLocalAlluxioCluster = new LocalAlluxioCluster(mNumWorkers);
    // Init configuration for integration test
    mLocalAlluxioCluster.initConfiguration();
    // Overwrite the test configuration with test specific parameters
    for (Entry<PropertyKey, String> entry : mConfiguration.entrySet()) {
      Configuration.set(entry.getKey(), entry.getValue());
    }
    // Start the cluster
    mLocalAlluxioCluster.start();
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    try {
      boolean startCluster = mStartCluster;
      Annotation configAnnotation = description.getAnnotation(Config.class);
      if (configAnnotation != null) {
        Config config = (Config) configAnnotation;
        // Override the configuration parameters with any configuration params
        for (int i = 0; i < config.confParams().length; i += 2) {
          mConfiguration.put(PropertyKey.fromString(config.confParams()[i]),
              config.confParams()[i + 1]);
        }
        // Override startCluster
        startCluster = config.startCluster();
      }
      if (startCluster) {
        start();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        try {
          statement.evaluate();
        } finally {
          mLocalAlluxioCluster.stop();
        }
      }
    };
  }

  /**
   * Builder for a {@link LocalAlluxioClusterResource}.
   */
  public static class Builder {
    private boolean mStartCluster;
    private int mNumWorkers;
    private Map<PropertyKey, String> mConfiguration;

    /**
     * Constructs the builder with default values.
     */
    public Builder() {
      mStartCluster = true;
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
      return new LocalAlluxioClusterResource(mStartCluster, mNumWorkers, mConfiguration);
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
}
