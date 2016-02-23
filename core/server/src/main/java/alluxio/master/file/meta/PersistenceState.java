/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file.meta;

import javax.annotation.concurrent.ThreadSafe;

/**
 * The persistence state of a file in the under-storage system.
 */
@ThreadSafe
public enum PersistenceState {
  NOT_PERSISTED, // file not persisted in the under FS
  IN_PROGRESS, // used for async persistence, the async persistence is scheduled and in progress
  PERSISTED, // the file is persisted in the under FS
  LOST // the file is lost but not persisted in the under FS
}
