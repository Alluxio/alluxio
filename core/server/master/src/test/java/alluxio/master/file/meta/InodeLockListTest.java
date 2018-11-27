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

package alluxio.master.file.meta;

import alluxio.master.file.options.CreateDirectoryOptions;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Unit tests for {@link InodeLockList}.
 */
public class InodeLockListTest {
  private static final InodeDirectory ROOT =
      InodeDirectory.create(0, -1, "", CreateDirectoryOptions.defaults());
  private static final InodeDirectory ROOT_CHILD =
      InodeDirectory.create(1, 0, "1", CreateDirectoryOptions.defaults());

  private static final InodeDirectory DISCONNECTED =
      InodeDirectory.create(2, 100, "dc", CreateDirectoryOptions.defaults());
  private static final InodeDirectory NONEXIST =
      InodeDirectory.create(3, 0, "3", CreateDirectoryOptions.defaults());

  private static final Predicate<Long> EXISTS_FN = Arrays.asList(0L, 1L, 2L)::contains;

  @Rule
  public ExpectedException mExpectedException = ExpectedException.none();

  private InodeLockManager mInodeLockManager = new InodeLockManager();
  private InodeLockList mLockList = new InodeLockList(mInodeLockManager);
  //TODO(andrew): add tests
}
