package alluxio.cli.validation;

import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.conf.InstancedConfiguration;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.*;

public class NativeLibValidationTaskTest {
  private String mLibPath;

  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void prepareConf() {
    sConf = InstancedConfiguration.defaults();
  }

  @Before
  public void storeJavaLibPath() {
    mLibPath = System.getProperty(NativeLibValidationTask.NATIVE_LIB_PATH);
  }

  @Test
  public void nativeLibPresent() throws Exception {
    File testLibDir = InfoCollectorTestUtils.createTemporaryDirectory();
    String testLibPath = testLibDir.getPath();

    String libPath = testLibPath;
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, libPath);

    NativeLibValidationTask task = new NativeLibValidationTask(sConf);
    ValidationTask.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidationTask.State.OK, result.mState);
  }

  @Test
  public void nativeLibMissing() throws Exception {
    String libPath = "/usr/missing";
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, libPath);

    NativeLibValidationTask task = new NativeLibValidationTask(sConf);
    ValidationTask.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidationTask.State.WARNING, result.mState);
    assertThat(result.mOutput, containsString("Java native lib not found at /usr/missing"));
    assertThat(result.mAdvice, containsString("Please check /usr/missing"));
  }

  @After
  public void resetJavaLibPath() {
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, mLibPath);
  }
}
