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

package alluxio.stress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import org.junit.Test;

import java.util.List;

public class BaseParametersTest {
  @ParametersDelegate
  private BaseParameters mBaseParameter = new BaseParameters();

  @Test
  public void parseParametersToArgumentString() throws Exception {
    JCommander jc = new JCommander(this);
    String[] inputArgs = new String[]{
        // arguments appear in pair (8 pairs)
        "--cluster-limit", "4",
        "--cluster-start-delay", "5s",
        "--profile-agent", "TestProfile",
        "--bench-timeout", "10m",
        "--id", "TestID",
        "--start-ms", "1000",
        "--java-opt", " TestOption1",
        "--java-opt", " TestOption2",
        // arguments appear alone
        "--cluster",
        "--distributed",
        "--in-process",
        "--help"
    };

    jc.parse(inputArgs);
    List<String> outputArgs = mBaseParameter.toBatchTaskArgumentString();

    assertEquals(outputArgs.size(), inputArgs.length);

    // for those appear in pair (8 pair), make sure they appear in the output in certain order
    for (int i = 0; i < 8; i++) {
      boolean found = false;
      for (int j = 0; j < outputArgs.size(); j++) {
        if (inputArgs[2 * i].equals(outputArgs.get(j))
            && inputArgs[2 * i + 1].equals(outputArgs.get(j + 1))) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }

    // for those appear alone
    for (int i = 16; i < inputArgs.length; i++) {
      assertTrue(outputArgs.contains(inputArgs[i]));
    }
  }
}
