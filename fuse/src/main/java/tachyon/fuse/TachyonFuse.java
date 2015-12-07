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

package tachyon.fuse;

import java.nio.file.Paths;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.client.ClientContext;
import tachyon.conf.TachyonConf;

/**
 * Main entry point to Tachyon-FUSE
 */
public final class TachyonFuse {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);
  private static TachyonConf sTachyonConf;

  public static void main(String[] args) {
    sTachyonConf = ClientContext.getConf();
    final TachyonFuseOptions opts = parseOptions(args);
    if (opts == null) {
      System.exit(1);
    }

    final TachyonFuseFs fs = new TachyonFuseFs(sTachyonConf, opts);
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

    final Option fuseOption = Option.builder("o")
        .valueSeparator(',')
        .required(false)
        .hasArgs()
        .desc("FUSE mount options")
        .build();

    opts.addOption(mntPoint);
    opts.addOption(tachyonRoot);
    opts.addOption(help);
    opts.addOption(fuseOption);

    final CommandLineParser parser = new DefaultParser();
    try {
      CommandLine cli = parser.parse(opts, args);

      if (cli.hasOption("h")) {
        final HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp(TachyonFuse.class.getName(), opts);
        return null;
      }

      String mntPointValue = cli.getOptionValue("m");
      String tachyonRootValue = cli.getOptionValue("r");

      List<String> fuseOpts = Lists.newArrayList();
      boolean noUserMaxWrite = true;
      if (cli.hasOption("o")) {
        String[] fopts = cli.getOptionValues("o");
        // keep the -o
        for (int i = 0; i < fopts.length; i++) {
          fuseOpts.add("-o" + fopts[i]);
          if (noUserMaxWrite && fopts[i].startsWith("max_write")) {
            noUserMaxWrite = false;
          }
        }
      }
      // check if the user has specified his own max_write, otherwise get it
      // from conf
      if (noUserMaxWrite) {
        final long maxWrite = sTachyonConf.getLong(Constants.FUSE_MAXWRITE_BYTES);
        fuseOpts.add(String.format("-omax_write=%d", maxWrite));
      }

      if (mntPointValue == null) {
        mntPointValue = sTachyonConf.get(Constants.FUSE_DEFAULT_MOUNTPOINT);
        LOG.info("Mounting on default {}", mntPointValue);
      }

      if (tachyonRootValue == null) {
        tachyonRootValue = sTachyonConf.get(Constants.FUSE_FS_ROOT);
        LOG.info("Using default tachyon root {}", tachyonRootValue);
      }

      final boolean fuseDebug = sTachyonConf.getBoolean(Constants.FUSE_DEBUG_ENABLE);

      return new TachyonFuseOptions(mntPointValue, tachyonRootValue, fuseDebug, fuseOpts);
    } catch (ParseException e) {
      System.err.println("Error while parsing CLI: " + e.getMessage());
      final HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp(TachyonFuse.class.getName(), opts);
      return null;
    }
  }
}
