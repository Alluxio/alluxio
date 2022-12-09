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

package alluxio.fuse;

import java.util.ArrayList;
import java.util.List;

/**
 * Constants for FUSE.
 */
public class FuseConstants {
  // Operation metric name
  public static final String FUSE_GETATTR = "Fuse.Getattr";
  public static final String FUSE_READDIR = "Fuse.Readdir";
  public static final String FUSE_READ = "Fuse.Read";
  public static final String FUSE_WRITE = "Fuse.Write";
  public static final String FUSE_MKDIR = "Fuse.Mkdir";
  public static final String FUSE_UNLINK = "Fuse.Unlink";
  public static final String FUSE_RMDIR = "Fuse.Rmdir";
  public static final String FUSE_RENAME = "Fuse.Rename";
  public static final String FUSE_CHMOD = "Fuse.Chmod";
  public static final String FUSE_CHOWN = "Fuse.Chown";
  public static final String FUSE_TRUNCATE = "Fuse.Truncate";

  /**
   * @return the selected FUSE method name for update check
   */
  public static List<String> getFuseMethodNames() {
    List<String> names = new ArrayList<>();
    names.add(FUSE_GETATTR);
    names.add(FUSE_READDIR);
    names.add(FUSE_READ);
    names.add(FUSE_WRITE);
    names.add(FUSE_MKDIR);
    names.add(FUSE_UNLINK);
    names.add(FUSE_RMDIR);
    names.add(FUSE_RENAME);
    names.add(FUSE_CHMOD);
    names.add(FUSE_CHOWN);
    names.add(FUSE_TRUNCATE);
    return names;
  }
}
