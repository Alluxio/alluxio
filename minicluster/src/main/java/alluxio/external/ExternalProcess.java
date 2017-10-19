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

package alluxio.external;

import alluxio.PropertyKey;
import alluxio.util.io.PathUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Class for launching another class in a new processes.
 */
@ThreadSafe
public class ExternalProcess implements Closeable {
  private final Map<PropertyKey, Object> mConf;
  private final Class<?> mClazz;
  private final File mOutFile;

  private Process mProcess;

  /**
   * @param conf alluxio configuration properties for the process
   * @param clazz the class to run
   * @param outfile the file to write process output to
   */
  public ExternalProcess(Map<PropertyKey, Object> conf, Class<?> clazz, File outfile) {
    mConf = conf;
    mClazz = clazz;
    mOutFile = outfile;
  }

  /**
   * Starts the process.
   */
  public synchronized void start() throws IOException {
    String java = PathUtils.concatPath(System.getProperty("java.home"), "bin", "java");
    String classpath = System.getProperty("java.class.path");
    List<String> args = new ArrayList<>(Arrays.asList(java, "-cp", classpath));
    for (Entry<PropertyKey, Object> entry : mConf.entrySet()) {
      args.add(String.format("-D%s=%s", entry.getKey().toString(), entry.getValue()));
    }
    args.add(mClazz.getCanonicalName());
    ProcessBuilder pb = new ProcessBuilder(args);
    pb.redirectError(mOutFile);
    pb.redirectOutput(mOutFile);
    mProcess = pb.start();
  }

  @Override
  public synchronized void close() {
    if (mProcess != null) {
      mProcess.destroy();
      mProcess = null;
    }
  }
}
