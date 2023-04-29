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

package alluxio.master.file;

/**
 * The policy that decides when an inode is created, if the last modified time of
 * its parent should be updated.
 * If its parent does not exist, the last existent directory in the inode path will
 * be updated instead.
 * For example, when /a/b/c is about to create and only /a exists, the last modified time
 * of /a will be updated, instead of /b. The missing components will be created along the
 * the path traversal.
 */
public enum ParentLastModifiedTimeUpdatePolicy {
  // The last modified time of the parent directory is always updated
  // when a direct inode under it is created.
  // This is the default behavior before alluxio 2.10.
  ALWAYS,

  // The last modified time of the parent directory will never be updated
  // upon inode creation.
  // Note that the last modified time will still be updated if the metadata
  // of an inode itself gets updated.
  NEVER,

  // Only update the parent last modified time if
  // the inode creation does not happen in metadata sync.
  // Metadata sync loads the file from UFS and creates the corresponding
  // inodes in alluxio inode tree.
  // Technically the parent directory can be considered not being updated,
  // because the new created inode was loaded from UFS instead of created organically.
  NON_METADATA_SYNC_INODE_CREATIONS_ONLY
}
