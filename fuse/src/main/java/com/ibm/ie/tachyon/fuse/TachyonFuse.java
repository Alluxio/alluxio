/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.ibm.ie.tachyon.fuse;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry point to Tachyon-FUSE
 */
public final class TachyonFuse {
  private static final Logger LOG = LoggerFactory.getLogger(TachyonFuse.class);
  private static final String DEFAULT_MOUNT_POINT = "/mnt/tachyon";
  private static final String DEFAULT_MASTER_ADDR = "tachyon://localhost:19998";
  private static final String DEFAULT_ROOT = "/";

  public static void main(String[] args) {
    final TachyonFuseOptions opts = parseOptions(args);
    if (opts == null) {
      System.exit(1);
    }
    final TachyonFuseFs fs = new TachyonFuseFs(opts);
    final List<String> fuseOpts = opts.getFuseOpts();
    fuseOpts.add("-odirect_io");

    try {
      fs.mount(Paths.get(opts.getMountPoint()), true, opts.isDebug(),
          fuseOpts.toArray(new String[0]));
    } finally {
      fs.umount();
    }
  }

  /**
   * Parses CLI options
   * @param args CLI args
   * @return Tachyon-FUSE configuration options
   */
  private static TachyonFuseOptions parseOptions(String[] args) {
    final Options opts = new Options();
    final Option mntPoint = Option.builder("m")
        .hasArg()
        .required(false)
        .longOpt("mount-point")
        .desc("Path where tachyon-fuse should be mounted.")
        .build();

    final Option tachyonAddress = Option.builder("t")
        .hasArg()
        .required(false)
        .longOpt("tachyon-master")
        .desc("URI of the Tachyon Master (e.g. tachyon://localhost:19998/")
        .build();

    final Option tachyonRoot = Option.builder("r")
        .hasArg()
        .required(false)
        .longOpt("tachyon-root")
        .desc("Path within tachyon that will be the root of the mount (e.g., /users/foo)")
        .build();

    final Option help = Option.builder("h")
        .required(false)
        .desc("Print this help")
        .build();

    final Option debug = Option.builder("d")
        .required(false)
        .longOpt("debug")
        .desc("Enable FUSE debug output")
        .build();

    final Option fuseOption = Option.builder("o")
        .valueSeparator(',')
        .required(false)
        .hasArgs()
        .desc("FUSE mount options")
        .build();

    opts.addOption(mntPoint);
    opts.addOption(tachyonAddress);
    opts.addOption(tachyonRoot);
    opts.addOption(help);
    opts.addOption(debug);
    opts.addOption(fuseOption);

    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(opts, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(TachyonFuse.class.getName(), opts);
        return null;
      }

      String m = cli.getOptionValue("m");
      String t = cli.getOptionValue("t");
      String r = cli.getOptionValue("r");
      boolean d = cli.hasOption("d");

      List<String> fuseOpts;
      if (cli.hasOption("o")) {
        String[] fopts = cli.getOptionValues("o");
        fuseOpts = new ArrayList<String>(fopts.length);
        // keep the -o
        for (int i = 0; i < fopts.length; i++) {
          fuseOpts.add("-o" + fopts[i]);
        }
      } else {
        fuseOpts = Collections.emptyList();
      }

      if (m == null) {
        LOG.info("Mounting on default {}", TachyonFuse.DEFAULT_MOUNT_POINT);
        m = TachyonFuse.DEFAULT_MOUNT_POINT;
      }

      if (t == null) {
        LOG.info("Using default master address {}", TachyonFuse.DEFAULT_MASTER_ADDR);
        t = TachyonFuse.DEFAULT_MASTER_ADDR;
      }

      if (r == null) {
        LOG.info("Using default tachyon root {}", TachyonFuse.DEFAULT_ROOT);
        r = TachyonFuse.DEFAULT_ROOT;
      }

      return new TachyonFuseOptions(m, t, r, d, fuseOpts);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(TachyonFuse.class.getName(), opts);
      return null;
    }

  }
}

