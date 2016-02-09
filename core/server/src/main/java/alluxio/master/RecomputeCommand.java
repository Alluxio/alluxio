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

package alluxio.master;

import alluxio.Constants;

import com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The recompute command class. Used to execute the recomputation.
 */
@ThreadSafe
public class RecomputeCommand implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mCommand;
  private final String mFilePath;

  /**
   * Creates a new {@link RecomputeCommand}.
   *
   * @param cmd the command to execute
   * @param filePath the path of the output file, which records the output of the recompute process
   */
  public RecomputeCommand(String cmd, String filePath) {
    mCommand = cmd;
    mFilePath = filePath;
  }

  @Override
  public void run() {
    try {
      LOG.info("Exec {} output to {}", mCommand, mFilePath);
      Process p = java.lang.Runtime.getRuntime().exec(mCommand);
      String line;
      Closer closer = Closer.create();
      try {
        BufferedReader bri =
            closer.register(new BufferedReader(new InputStreamReader(p.getInputStream())));
        BufferedReader bre =
            closer.register(new BufferedReader(new InputStreamReader(p.getErrorStream())));
        File file = new File(mFilePath);
        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = closer.register(new BufferedWriter(fw));
        while ((line = bri.readLine()) != null) {
          bw.write(line + "\n");
        }
        while ((line = bre.readLine()) != null) {
          bw.write(line + "\n");
        }
        bw.flush();
      } finally {
        closer.close();
      }

      p.waitFor();
      LOG.info("Exec {} output to {} done.", mCommand, mFilePath);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
  }
}
