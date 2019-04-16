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

package alluxio.multi.process;

import alluxio.conf.PropertyKey;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for running and interacting with an Alluxio worker in a separate process.
 */
@ThreadSafe
public class Worker implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(Worker.class);

  private final File mLogsDir;
  private final Map<PropertyKey, String> mProperties;

  private ExternalProcess mProcess;

  /**
   * @param logsDir log directory
   * @param properties alluxio properties
   */
  public Worker(File logsDir, Map<PropertyKey, String> properties) throws IOException {
    mLogsDir = logsDir;
    mProperties = properties;
  }

  /**
   * Launches the worker process.
   */
  public synchronized void start() throws IOException {
    Preconditions.checkState(mProcess == null, "Worker is already running");
    mProcess = new ExternalProcess(mProperties, LimitedLifeWorkerProcess.class,
        new File(mLogsDir, "worker.out"));
    mProcess.start();
  }

  @Override
  public synchronized void close() {
    if (mProcess != null) {
      mProcess.stop();
      mProcess = null;
    }
  }
}
