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

package tachyon.shell.command;

import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.file.TachyonFileSystem;
import tachyon.client.file.options.SetAclOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Parent class for commands: chown, chownr, chgrp, chgrpr, chmod and chmodr.
 */
public abstract class AbstractACLCommand extends AbstractTfsShellCommand {

  protected AbstractACLCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  /**
   * Changes the owner for the directory or file with the path specified in args.
   *
   * @param path The {@link TachyonURI} path as the input of the command
   * @param owner The owner to be updated to the file or directory
   * @param recursive Whether change the owner recursively
   * @throws IOException if command failed
   */
  protected void chown(TachyonURI path, String owner, boolean recursive) throws IOException {
    try {
      SetAclOptions options =
          new SetAclOptions.Builder().setOwner(owner).setRecursive(recursive).build();
      mTfs.setAcl(path, options);
      System.out.println("Changed owner of " + path + " to " + owner);
    } catch (TachyonException e) {
      throw new IOException("Failed to changed owner of " + path + " to " + owner + " : "
          + e.getMessage());
    }
  }

  /**
   * Changes the group for the directory or file with the path specified in args.
   *
   * @param path The {@link TachyonURI} path as the input of the command
   * @param group The group to be updated to the file or directory
   * @param recursive Whether change the group recursively
   * @throws IOException
   */
  protected void chgrp(TachyonURI path, String group, boolean recursive) throws IOException {
    try {
      SetAclOptions options =
          new SetAclOptions.Builder().setGroup(group).setRecursive(recursive).build();
      mTfs.setAcl(path, options);
      System.out.println("Changed group of " + path + " to " + group);
    } catch (TachyonException e) {
      throw new IOException("Failed to changed group of " + path + " to " + group + " : "
          + e.getMessage());
    }
  }

  /**
   * Changes the permissions of directory or file with the path specified in args.
   *
   * @param path The {@link TachyonURI} path as the input of the command
   * @param modeStr The new permission to be updated to the file or directory
   * @param recursive Whether change the permission recursively
   * @throws IOException if command failed
   */
  protected void chmod(TachyonURI path, String modeStr, boolean recursive) throws IOException {
    short newPermission = 0;
    try {
      newPermission = Short.parseShort(modeStr);
      SetAclOptions options =
          new SetAclOptions.Builder().setPermission(newPermission).setRecursive(recursive).build();
      mTfs.setAcl(path, options);
      System.out.println("Changed permission of " + path + " to " + newPermission);
    } catch (Exception e) {
      throw new IOException("Failed to changed permission of  " + path + " to " + newPermission
          + " : " + e.getMessage());
    }
  }
}
