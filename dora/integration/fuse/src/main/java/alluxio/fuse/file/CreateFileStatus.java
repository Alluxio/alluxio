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

package alluxio.fuse.file;

import alluxio.fuse.AlluxioFuseUtils;
import alluxio.fuse.auth.AuthPolicy;

import com.google.common.base.Preconditions;

/**
 * The file status of ongoing fuse out stream.
 */
public class CreateFileStatus extends FileStatus {
  private final long mMode;
  private final long mUid;
  private final long mGid;

  /**
   * Creates a {@link CreateFileStatus}.
   *
   * @param authPolicy the authentication policy
   * @param mode the mode
   * @param fileLength the file length
   * @return the {@link CreateFileStatus}
   */
  public static CreateFileStatus create(AuthPolicy authPolicy, long mode, long fileLength) {
    Preconditions.checkNotNull(authPolicy);
    long uid = authPolicy.getUid().orElse(AlluxioFuseUtils.ID_NOT_SET_VALUE);
    long gid = authPolicy.getGid().orElse(AlluxioFuseUtils.ID_NOT_SET_VALUE);
    return new CreateFileStatus(fileLength, mode, uid, gid);
  }

  /**
   * Constructs a new {@link CreateFileStatus}.
   *
   * @param fileLength the file length
   * @param mode the mode
   * @param uid the uid
   * @param gid the gid
   */
  public CreateFileStatus(long fileLength, long mode, long uid, long gid) {
    super(fileLength);
    mMode = mode;
    mUid = uid;
    mGid = gid;
  }

  /**
   * @return the mode
   */
  public long getMode() {
    return mMode;
  }

  /**
   * @return the uid
   */
  public long getUid() {
    return mUid;
  }

  /**
   * @return the gid
   */
  public long getGid() {
    return mGid;
  }
}
