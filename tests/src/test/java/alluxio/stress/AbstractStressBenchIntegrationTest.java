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

import static org.junit.Assert.assertTrue;

import alluxio.stress.cli.report.GenerateReport;
import alluxio.testutils.BaseIntegrationTest;
import alluxio.testutils.LocalAlluxioClusterResource;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Base functionality for stress integration tests.
 */
public class AbstractStressBenchIntegrationTest extends BaseIntegrationTest {
  @ClassRule
  public static LocalAlluxioClusterResource sLocalAlluxioClusterResource =
      new LocalAlluxioClusterResource.Builder().build();

  @Rule
  public TestRule mResetRule = sLocalAlluxioClusterResource.getResetResource();

  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  protected void generateAndVerifyReport(List<String> expectedGraphNames, String... outputJson)
      throws Exception {
    List<String> args = new ArrayList<>();

    // write out the jsons outputs to files (for the input of generate)
    for (String output : outputJson) {
      File input = mFolder.newFile();
      try (FileWriter writer = new FileWriter(input)) {
        writer.write(output);
      }
      args.add("--input");
      args.add(input.getAbsolutePath());
    }

    // generate the output
    File generatedReport =
        mFolder.newFile(String.format("report-%d.html", System.currentTimeMillis()));
    args.add("--output");
    args.add(generatedReport.getAbsolutePath());
    new GenerateReport().run(args.toArray(new String[0]));

    // validate the generated report
    String result = new String(Files.readAllBytes(Paths.get(generatedReport.getAbsolutePath())));
    for (String expectedName : expectedGraphNames) {
      assertTrue("report must contain graph name: " + expectedName, result.contains(expectedName));
    }
  }
}
