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

/**
 * Object representation of a remote scp command.
 * The scp command copies a file/dir from remote host to local.
 */
@NotThreadSafe
public class ScpCommand extends ShellCommand {
  private final String mHostName;

  /**
   * Creates a remote scp command to copy a file to local.
   *
   * @param remoteHost remote hostname
   * @param fromFile the remote file
   * @param toFile target location to copy to
   */
  public ScpCommand(String remoteHost, String fromFile, String toFile) {
    this(remoteHost, fromFile, toFile, false);
  }

  /**
   * Creates a remote scp command to copy a file/dir to local.
   *
   * @param remoteHost the remote hostname
   * @param fromFile the remote file/dir to copy
   * @param toFile target path to copy to
   * @param isDir if true, the remote file is a directory
   */
  public ScpCommand(String remoteHost, String fromFile, String toFile, boolean isDir) {
    super(new String[]{"bash", "-c",
            String.format(isDir ? "scp -r %s %s:%s %s" : "scp %s %s:%s %s",
                    ShellUtils.COMMON_SSH_OPTS, remoteHost, fromFile, toFile)
    });
    mHostName = remoteHost;
  }

  /**
   * Returns the remote target hostname.
   *
   * @return target hostname
   */
  public String getHostName() {
    return mHostName;
  }
}
