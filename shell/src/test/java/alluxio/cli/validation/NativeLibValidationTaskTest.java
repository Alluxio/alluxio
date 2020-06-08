package alluxio.cli.validation;

import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import alluxio.cli.ValidateUtils;
import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.conf.InstancedConfiguration;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

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
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.OK, result.getState());
  }

  @Test
  public void nativeLibMissing() throws Exception {
    String libPath = "/usr/missing";
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, libPath);

    NativeLibValidationTask task = new NativeLibValidationTask(sConf);
    ValidateUtils.TaskResult result = task.validate(ImmutableMap.of());
    System.out.println(result);
    assertEquals(ValidateUtils.State.WARNING, result.getState());
    assertThat(result.getResult(), containsString("Java native lib not found at /usr/missing"));
    assertThat(result.getAdvice(), containsString("Please check your path /usr/missing"));
  }

  @After
  public void resetJavaLibPath() {
    System.setProperty(NativeLibValidationTask.NATIVE_LIB_PATH, mLibPath);
  }
}
