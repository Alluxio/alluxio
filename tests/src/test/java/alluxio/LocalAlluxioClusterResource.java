/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.google.common.base.Preconditions;

import alluxio.exception.ConnectionFailedException;
import alluxio.master.LocalAlluxioCluster;

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
  /** Configuration parameters for the {@link Configuration} object used in the cluster. */
  private final String[] mConfParams;

  /** The Alluxio cluster being managed. */
  private LocalAlluxioCluster mLocalAlluxioCluster = null;
  /** The {@link Configuration} object used by the cluster. */
  private Configuration mTestConf = null;

  /**
   * Create a new instance.
   *
   * @param workerCapacityBytes the capacity of the worker in bytes
   * @param userBlockSize the block size for a user
   * @param startCluster whether or not to start the cluster before the test method starts
   * @param confParams specific alluxio configuration parameters, specified as a list of strings,
   */
  public LocalAlluxioClusterResource(long workerCapacityBytes, int userBlockSize,
                                     boolean startCluster, String... confParams) {
    Preconditions.checkArgument(confParams.length % 2 == 0);
    mWorkerCapacityBytes = workerCapacityBytes;
    mUserBlockSize = userBlockSize;
    mStartCluster = startCluster;
    mConfParams = confParams;
  }

  /**
   * Create a new {@link LocalAlluxioClusterResource} with default configuration.
   */
  // TODO(andrew) Go through our integration tests and see how many can use this constructor.
  public LocalAlluxioClusterResource() {
    this(DEFAULT_WORKER_CAPACITY_BYTES, DEFAULT_USER_BLOCK_SIZE);
  }

  public LocalAlluxioClusterResource(long workerCapacityBytes, int userBlockSize,
                                     boolean startCluster) {
    this(workerCapacityBytes, userBlockSize, startCluster, new String[0]);
  }

  public LocalAlluxioClusterResource(long workerCapacityBytes, int userBlockSize,
                                     String... confParams) {
    this(workerCapacityBytes, userBlockSize, true, confParams);
  }

  public LocalAlluxioClusterResource(long workerCapacityBytes, int userBlockSize) {
    this(workerCapacityBytes, userBlockSize, true, new String[0]);
  }

  /**
   * @return the {@link LocalAlluxioCluster} being managed
   */
  public LocalAlluxioCluster get() {
    return mLocalAlluxioCluster;
  }

  /**
   * @return the {@link Configuration} object used by the cluster
   */
  public Configuration getTestConf() {
    return mTestConf;
  }

  /**
   * Explicitly starts the {@link LocalAlluxioCluster}.
   *
   * @throws IOException if an I/O error occurs
   * @throws ConnectionFailedException if network connection failed
   */
  public void start() throws IOException, ConnectionFailedException {
    mLocalAlluxioCluster.start(mTestConf);
  }

  @Override
  public Statement apply(final Statement statement, Description description) {
    mLocalAlluxioCluster = new LocalAlluxioCluster(mWorkerCapacityBytes, mUserBlockSize);
    try {
      mTestConf = mLocalAlluxioCluster.newTestConf();
      // Override the configuration parameters with mConfParams
      for (int i = 0; i < mConfParams.length; i += 2) {
        mTestConf.set(mConfParams[i], mConfParams[i + 1]);
      }

      boolean startCluster = mStartCluster;
      Annotation configAnnotation = description.getAnnotation(Config.class);
      if (configAnnotation != null) {
        Config config = (Config) configAnnotation;
        // Override the configuration parameters with any configuration params
        for (int i = 0; i < config.confParams().length; i += 2) {
          mTestConf.set(config.confParams()[i], config.confParams()[i + 1]);
        }
        // Override startCluster
        startCluster = config.startCluster();
      }
      if (startCluster) {
        mLocalAlluxioCluster.start(mTestConf);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (ConnectionFailedException e) {
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
