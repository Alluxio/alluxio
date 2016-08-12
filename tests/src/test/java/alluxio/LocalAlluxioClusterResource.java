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

import alluxio.exception.AlluxioException;
import alluxio.master.LocalAlluxioCluster;

import com.google.common.base.Preconditions;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A JUnit Rule resource for automatically managing a local alluxio cluster for testing. To use it,
 * create an instance of the class under a {@literal@}Rule annotation, with the required
 * configuration parameters, and any necessary explicit {@link Configuration} settings. The Alluxio
 * cluster will be set up from scratch at the end of every method (or at the start of every suite if
 * {@literal@}ClassRule is used), and destroyed at the end. Below is an example of declaring and
 * using it.
 *
 * <pre>
 *   public class SomethingTest {
 *    {@literal@}Rule
 *    public LocalAlluxioClusterResource localAlluxioClusterResource =
 *      new LocalAlluxioClusterResource(
 *        WORKER_CAPACITY, QUOTA_UNIT, BLOCK_SIZE, CONF_KEY_1, CONF_VALUE_1, ...);
 *
 *    {@literal@}Test
 *    public void testSomething() {
 *      localAlluxioClusterResource.get().getClient().create("/abced");
 *      ...
 *    }
 *
 *    {@literal@}Test
 *    {@literal@}Config(alluxioConfParams = {CONF_KEY_1, CONF_VALUE_1, CONF_KEY_2,
 *                                           CONF_VALUE_2, ...}
 *                      startCluster = false)
 *    public void testSomethingElse() {
 *      localAlluxioClusterResource.start();
 *      localAlluxioClusterResource.get().getClient().create("/efghi");
 *      ...
 *    }
 *   }
 * </pre>
 */
@NotThreadSafe
public final class LocalAlluxioClusterResource implements TestRule {
  /** Default worker capacity in bytes. */
  public static final long DEFAULT_WORKER_CAPACITY_BYTES = 100 * Constants.MB;
  /** Default block size in bytes. */
  public static final int DEFAULT_USER_BLOCK_SIZE = Constants.KB;

  /** The capacity of the worker in bytes. */
  private final long mWorkerCapacityBytes;
  /** Block size for a user. */
  private final int mUserBlockSize;
  /**
   * If true (default), we start the cluster before running a test method. Otherwise, the method
   * must start the cluster explicitly.
   */
  private final boolean mStartCluster;

  /** Configuration keys for the {@link Configuration} object used in the cluster. */
  private final List<PropertyKey> mConfKeys = new ArrayList<>();
  /** Configuration values for the {@link Configuration} object used in the cluster. */
  private final List<String> mConfValues = new ArrayList<>();

  /** The Alluxio cluster being managed. */
  private LocalAlluxioCluster mLocalAlluxioCluster = null;

  /**
   * Creates a new instance.
   *
   * @param workerCapacityBytes the capacity of the worker in bytes
   * @param userBlockSize the block size for a user
   * @param startCluster whether or not to start the cluster before the test method starts
   */
  public LocalAlluxioClusterResource(long workerCapacityBytes, int userBlockSize,
      boolean startCluster) {
    mWorkerCapacityBytes = workerCapacityBytes;
    mUserBlockSize = userBlockSize;
    mStartCluster = startCluster;
  }

  /**
   * Creates a new {@link LocalAlluxioClusterResource} with default configuration.
   */
  // TODO(andrew) Go through our integration tests and see how many can use this constructor.
  public LocalAlluxioClusterResource() {
    this(DEFAULT_WORKER_CAPACITY_BYTES, DEFAULT_USER_BLOCK_SIZE);
  }

  public LocalAlluxioClusterResource(long workerCapacityBytes, int userBlockSize) {
    this(workerCapacityBytes, userBlockSize, true);
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
    mConfKeys.add(key);
    mConfValues.add(value.toString());
    return this;
  }

  /**
   * Explicitly starts the {@link LocalAlluxioCluster}.
   */
  public void start() throws IOException, AlluxioException {
    // Init configuration for integration test
    mLocalAlluxioCluster.initConfiguration();
    // Overwrite the test configuration with test specific parameters
    for (int i = 0; i < mConfKeys.size(); i++) {
      Configuration.set(mConfKeys.get(i), mConfValues.get(i));
    }
    // Start the cluster
    mLocalAlluxioCluster.start();
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    mLocalAlluxioCluster = new LocalAlluxioCluster(mWorkerCapacityBytes, mUserBlockSize);
    try {
      boolean startCluster = mStartCluster;
      Annotation configAnnotation = description.getAnnotation(Config.class);
      if (configAnnotation != null) {
        Config config = (Config) configAnnotation;
        // Override the configuration parameters with any configuration params
        for (int i = 0; i < config.confParams().length; i += 2) {
          setProperty(PropertyKey.fromString(config.confParams()[i]), config.confParams()[i + 1]);
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
   * An annotation for test methods that can be used to override any of the defaults set at
   * construction time.
   */
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Config {
    String[] confParams() default {};
    boolean startCluster() default true;
  }
}
