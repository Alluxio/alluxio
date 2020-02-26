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

package alluxio.cli.fs.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import alluxio.AlluxioURI;
import alluxio.SystemOutRule;
import alluxio.client.file.URIStatus;
import alluxio.wire.FileInfo;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DuCommandTest {

  private static final Pattern OUTPUT_PERCENT = Pattern.compile("[(]([0-9].*?)[%][)]");

  ByteArrayOutputStream mOutput = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mRule = new SystemOutRule(mOutput);

  @Before
  public void before() {
    mOutput.reset();
  }

  @Test
  public void allInMem() {
    AlluxioURI rootpath = new AlluxioURI("/");
    List<FileInfo> infos = new ArrayList<>();
    infos.add(
        new FileInfo().setLength(10).setInAlluxioPercentage(100).setInMemoryPercentage(100));
    infos.add(
        new FileInfo().setLength(30).setInAlluxioPercentage(100).setInMemoryPercentage(100));
    infos.add(
        new FileInfo().setLength(20).setInAlluxioPercentage(100).setInMemoryPercentage(100));

    List<URIStatus> statuses = infos.stream().map(URIStatus::new).collect(Collectors.toList());
    DuCommand.getSizeInfo(rootpath, statuses, false, true, true);
    String output = mOutput.toString().trim();
    assertPercentage(output, 100, 100);
  }

  @Test
  public void noneInMem() {
    AlluxioURI rootpath = new AlluxioURI("/");
    List<FileInfo> infos = new ArrayList<>();
    infos.add(
        new FileInfo().setLength(10).setInAlluxioPercentage(100).setInMemoryPercentage(0));
    infos.add(
        new FileInfo().setLength(30).setInAlluxioPercentage(100).setInMemoryPercentage(0));
    infos.add(
        new FileInfo().setLength(20).setInAlluxioPercentage(100).setInMemoryPercentage(0));

    List<URIStatus> statuses = infos.stream().map(URIStatus::new).collect(Collectors.toList());
    DuCommand.getSizeInfo(rootpath, statuses, false, true, true);
    String output = mOutput.toString().trim();
    assertPercentage(output, 100, 0);
  }

  @Test
  public void someInMem() {
    AlluxioURI rootpath = new AlluxioURI("/");
    List<FileInfo> infos = new ArrayList<>();
    infos.add(
        new FileInfo().setLength(10).setInAlluxioPercentage(100).setInMemoryPercentage(50));
    infos.add(
        new FileInfo().setLength(30).setInAlluxioPercentage(100).setInMemoryPercentage(100));
    infos.add(
        new FileInfo().setLength(20).setInAlluxioPercentage(50).setInMemoryPercentage(0));

    List<URIStatus> statuses = infos.stream().map(URIStatus::new).collect(Collectors.toList());
    DuCommand.getSizeInfo(rootpath, statuses, false, true, true);
    String output = mOutput.toString().trim();
    assertPercentage(output, 83, 58);
  }

  /**
   * Matches the output of the du command using a regex pattern.
   *
   * The input string to the
   * @param output the output string from du. If it only has one percentage in the output, it is
   *               assumed to be the in-alluxio percentage, otherwise it assumed there is both
   *               in-alluxio and in-memory
   * @param inAlluxio the expected inAlluxio percentage (0-100)
   * @param inMem the expected inMem percentage (0-100). Set as a negative value to ignore matching
   */
  private static void assertPercentage(String output, int inAlluxio, int inMem) {
    Matcher matches = OUTPUT_PERCENT.matcher(output);
    if (!matches.find()) {
      fail("no group matches for output did not match regex pattern.");
    }
    assertEquals(inAlluxio, Integer.parseInt(matches.group(1)));

    if (matches.find() && inMem >= 0) {
      assertEquals(inMem, Integer.parseInt(matches.group(1)));
    } else if (inMem >= 0) {
      fail("Expected to match inMem percentage but did not");
    }
  }
}
