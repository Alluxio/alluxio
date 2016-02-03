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

package tachyon.job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.annotation.concurrent.ThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.thrift.CommandLineJobInfo;
import tachyon.thrift.JobConfInfo;

/**
 * A job that wraps a programmed run by command line. This job's caller should ensure the execution
 * environment are identical on master and at the client side.
 */
@ThreadSafe
public class CommandLineJob extends Job {
  private static final long serialVersionUID = 1655996721855899996L;
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  private final String mCommand;

  /**
   * Creates a new instance of {@link CommandLineJob}.
   *
   * @param command the command that can run in shell
   * @param jobConf the job configuration
   */
  public CommandLineJob(String command, JobConf jobConf) {
    super(jobConf);
    mCommand = command;
  }

  /**
   * Generates the {@link CommandLineJobInfo} for the command.
   *
   * @return the {@link CommandLineJobInfo} for RPC
   */
  public CommandLineJobInfo generateCommandLineJobInfo() {
    return new CommandLineJobInfo(mCommand, new JobConfInfo(getJobConf().getOutputFilePath()));
  }

  /**
   * Gets the command.
   *
   * @return the command
   */
  public String getCommand() {
    return mCommand;
  }

  @Override
  public boolean run() {
    try {
      String outputPath = getJobConf().getOutputFilePath();
      LOG.info("Exec {} output to {}", mCommand, outputPath);
      Process p = java.lang.Runtime.getRuntime().exec(mCommand);
      String line;
      Closer closer = Closer.create();
      try {
        BufferedReader bri =
            closer.register(new BufferedReader(new InputStreamReader(p.getInputStream())));
        BufferedReader bre =
            closer.register(new BufferedReader(new InputStreamReader(p.getErrorStream())));
        File file = new File(outputPath);
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
      LOG.info("Exec {} output to {} done.", mCommand, outputPath);
    } catch (IOException e) {
      LOG.error(e.getMessage());
      return false;
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    return "Command line job, command:" + mCommand;
  }
}
