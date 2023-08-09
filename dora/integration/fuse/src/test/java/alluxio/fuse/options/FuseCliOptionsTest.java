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

package alluxio.fuse.options;

import static org.junit.Assert.*;

import com.beust.jcommander.JCommander;
import org.junit.Test;

public class FuseCliOptionsTest {
  @Test
  public void parse() throws Exception {
    FuseCliOptions cliOptions = new FuseCliOptions();
    JCommander jCommander = JCommander.newBuilder()
        .addObject(cliOptions)
        .build();
    jCommander.parse("-m", "/tmp/alluxio-fuse");
  }
}
