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

package alluxio.shell;

import alluxio.util.ShellUtils;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ScpCommand extends ShellCommand {
  protected String mHostName;

  private ScpCommand(String remoteHost, String fromFile, String toFile) {
    this(remoteHost, fromFile, toFile, false);
  }

  public ScpCommand(String remoteHost, String fromFile, String toFile, boolean isDir) {
    super(new String[]{});

    String template = "scp %s %s:%s localhost:%s";
    if (isDir) {
      template = "scp -r %s %s:%s localhost:%s";
    }

    // copy from hostname to local
    mCommand = new String[]{"bash", "-c",
            String.format(template, ShellUtils.COMMON_SSH_OPTS, remoteHost, fromFile, toFile)
    };
    mHostName = remoteHost;
  }

  protected String getHostName() {
    return mHostName;
  }
}
