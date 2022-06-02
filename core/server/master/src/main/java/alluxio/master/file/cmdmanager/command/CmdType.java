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

package alluxio.master.file.cmdmanager.command;

import alluxio.proto.journal.File;

/**
 * Command Types.
 */
public enum CmdType {
  LoadCmd;

  /** Convert to proto format.
   * @return proto representation of the status
   */
  public File.CommandType toProto() {
    if (this == CmdType.LoadCmd) {
      return File.CommandType.DIST_LOAD;
    }
    return File.CommandType.UNKNOWN;
  }

  /**
   * Convert proto format to CmdType.
   * @param type proto formatted type
   * @return command type
   */
  public static CmdType fromProto(File.CommandType type) {
    if (type == File.CommandType.DIST_LOAD) {
      return LoadCmd;
    } else {
      throw new IllegalStateException("Cannot recognize the command type from protobuf" + type);
    }
  }
}
