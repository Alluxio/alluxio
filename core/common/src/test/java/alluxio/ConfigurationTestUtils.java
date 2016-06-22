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

package alluxio;

import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Properties;

@RunWith(PowerMockRunner.class)
@PrepareForTest(Configuration.class)
public final class ConfigurationTestUtils {

  public static void resetConfiguration() {
    Properties properties = Whitebox.getInternalState(Configuration.class, "PROPERTIES");
    properties.clear();
    Configuration.defaultInit();
  }

  private ConfigurationTestUtils() {} // prevent instantiation
}
