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

package alluxio.cli;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class NativeLibValidationTaskTest {
  private String mLibPath;

  @Before
  public void storeJavaLibPath() {
    mLibPath = System.getProperty(NativeLibValidationTask.NATIVE_LIB_PATH);
  }

  @Test
  public void nativeLibPresent() throws Exception {
    File testLibDir = ValidationTestUtils.createTemporaryDirectory();
    String testLibPath = testLibDir.getPath();

    String libPath = testLibPath;
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, libPath);

    NativeLibValidationTask task = new NativeLibValidationTask();
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.OK, result.getState());
  }

  @Test
  public void nativeLibMissing() throws Exception {
    String libPath = "/usr/missing";
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, libPath);

    NativeLibValidationTask task = new NativeLibValidationTask();
    ValidationTaskResult result = task.validateImpl(ImmutableMap.of());
    assertEquals(ValidationUtils.State.WARNING, result.getState());
    assertThat(result.getResult(), containsString("Java native lib not found at /usr/missing"));
    assertThat(result.getAdvice(), containsString("Please check your path /usr/missing"));
  }

  @After
  public void resetJavaLibPath() {
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, mLibPath);
  }
}
