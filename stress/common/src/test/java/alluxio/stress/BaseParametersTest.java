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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParametersDelegate;
import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseParametersTest {
  @ParametersDelegate
  private BaseParameters mBaseParameter;

  public static final String ID_FLAG = "--id";
  public static final String JAVA_OPT_FLAG = "--java-opt";
  public static final String PROFILE_AGENT = "--profile-agent";

  @Before
  public void before() {
    mBaseParameter = new BaseParameters();
  }

  @Test
  public void parseParametersToArgumentALL() throws Exception {
    String[] inputArgs = new String[]{
        // keys with values (8 pairs)
        "--cluster-limit", "4",
        "--cluster-start-delay", "5s",
        "--profile-agent", "TestProfile",
        "--bench-timeout", "10m",
        "--id", "TestID",
        "--start-ms", "1000",
        "--java-opt", " TestOption1",
        "--java-opt", " TestOption2",
        // keys with no values
        "--cluster",
        "--distributed",
        "--in-process",
        "--help"
    };

    JCommander jc = new JCommander(this);
    jc.parse(inputArgs);
    List<String> outputArgs = mBaseParameter.toBatchTaskArgumentString();

    assertEquals(outputArgs.size(), inputArgs.length);
    validateOutput(Arrays.asList(inputArgs), outputArgs, 8);
  }

  @Test
  public void parseParametersToArgumentEmpty() throws Exception {
    // with an empty input, the output should be default value of base parameters
    String[] inputArgs = new String[0];
    JCommander jc = new JCommander(this);
    jc.parse(inputArgs);
    List<String> outputArgs = mBaseParameter.toBatchTaskArgumentString();

    String[] defaultArgs = new String[]{
        // keys with values (4 pairs)
        "--cluster-limit", "0",
        "--cluster-start-delay", "10s",
        "--bench-timeout", "20m",
        "--start-ms", "-1",
    };

    assertEquals(outputArgs.size(), 8);

    // the two special parameters should not be parsed
    assertFalse(outputArgs.contains(PROFILE_AGENT));
    assertFalse(outputArgs.contains(ID_FLAG));

    validateOutput(Arrays.asList(defaultArgs), outputArgs, 4);
  }

  @Test
  public void parseParameterToArgumentWithJavaOPT() {
    ImmutableList<String> options = ImmutableList.of(" TestOption1", " TestOption2",
        " TestOption3", " TestOption4", " TestOption5");

    List<String> inputArgs = new ArrayList<>(Arrays.asList(
        // keys with values
        "--cluster-limit", "4",
        "--cluster-start-delay", "5s",
        "--id", "TestID",
        // keys with no values
        "--cluster"));

    for (String s : options) {
      inputArgs.add("--java-opt");
      inputArgs.add(s);
    }

    JCommander jc = new JCommander(this);
    jc.parse(inputArgs.toArray(new String[0]));
    List<String> outputArgs = mBaseParameter.toBatchTaskArgumentString();

    // validate the --java-opt
    List<String> optionList = new ArrayList<>();
    for (int i = 0; i < outputArgs.size(); i++) {
      if (outputArgs.get(i).equals(JAVA_OPT_FLAG)) {
        optionList.add(outputArgs.get(i + 1));
      }
    }

    assertEquals(optionList.size(), 5);
    for (String option : optionList) {
      assertTrue(options.contains(option));
    }
  }

  @Test
  public void parseParameterToArgumentWithoutJavaOPT() {
    String[] inputArgs = new String[]{
        // keys with values
        "--cluster-limit", "4",
        "--cluster-start-delay", "5s",
        "--id", "TestID",
        // keys with no values
        "--cluster",
    };

    JCommander jc = new JCommander(this);
    jc.parse(inputArgs);
    List<String> outputArgs = mBaseParameter.toBatchTaskArgumentString();

    // validate the --java-opt
    assertFalse(outputArgs.contains(JAVA_OPT_FLAG));
  }

  @Test
  public void parseSingleParametersToArgument() throws Exception {
    // test single parameter
    List<String[]> inputArgs = Arrays.asList(
        new String[]{"--cluster-limit", "4"},
        new String[]{"--cluster-start-delay", "5s"},
        new String[]{"--profile-agent", "TestProfile"},
        new String[]{"--bench-timeout", "10m"},
        new String[]{"--id", "TestID"},
        new String[]{"--start-ms", "1000"},
        new String[]{"--distributed"},
        new String[]{"--in-process"},
        new String[]{"--help"}
    );

    for (String[] s : inputArgs) {
      mBaseParameter = new BaseParameters();
      JCommander jc = new JCommander(this);
      jc.parse(s);
      List<String> outputArgs = mBaseParameter.toBatchTaskArgumentString();
      if (s.length == 1) {
        assertTrue(outputArgs.contains(s[0]));
      }
      else {
        validateOutput(Arrays.asList(s), outputArgs, 1);
      }
    }
  }

  private void validateOutput(List<String> inputArgs, List<String> outputArgs,
      int argWithValueCnt) {
    // for those that appear in pairs, make sure they appear in the output in certain order
    for (int i = 0; i < argWithValueCnt; i++) {
      boolean found = false;
      for (int j = 0; j < outputArgs.size(); j++) {
        if (inputArgs.get(2 * i).equals(outputArgs.get(j))
            && inputArgs.get(2 * i + 1).equals(outputArgs.get(j + 1))) {
          found = true;
          break;
        }
      }
      assertTrue(found);
    }

    // for those that have no values
    for (int i = argWithValueCnt * 2; i < inputArgs.size(); i++) {
      assertTrue(outputArgs.contains(inputArgs.get(i)));
    }
  }
}
