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
/**
 * Tests for the UFS journal. These tests require an under file system but not a local Alluxio
 * cluster. Ideally they will become unit tests if we can mock the under file system or allow local
 * under file system to be accessed in common.
 */
package alluxio.master.journal.ufs;
