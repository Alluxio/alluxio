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

package alluxio.hub.agent.util.process;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A class representing query-able information about the running system's process table.
 */
public class LinuxProcessTable implements  ProcessTable {
  private static final Logger LOG = LoggerFactory.getLogger(LinuxProcessTable.class);

  @Override
  public Stream<Integer> getJavaPids() throws IOException {
    return getAllPids().filter(p -> {
      Path comm = Paths.get("/proc", p.toString(), "comm");
      // We can skip "self" as its just a symlink
      try {
        if (!Files.exists(comm)) {
          return false;
        }
        String s = new String(Files.readAllBytes(comm), Charset.defaultCharset());
        return s.toLowerCase().contains("java");
      } catch (IOException | NumberFormatException e) {
        LOG.debug("Failed to read {} for comm", comm, e);
      }
      return false;
    });
  }

  @Override
  public Stream<Integer> getAllPids() throws IOException {
    return Files.list(Paths.get("/proc"))
        .map(p -> {
          Path comm = p.resolve("comm");
          // We can skip "self" as its just a symlink
          try {
            Path fname = p.getFileName();
            if (fname == null) {
              return null;
            }
            return Integer.parseInt(fname.toString());
          } catch (NumberFormatException e) {
            LOG.debug("Failed to read {} for comm", comm, e);
          }
          return null;
        }).filter(Objects::nonNull);
  }

  @Override
  public List<Integer> getJavaPid(@Nullable Stream<Integer> possiblePids, Class<?> clazz)
          throws IOException {
    if (possiblePids == null) {
      possiblePids = getJavaPids();
    }
    return possiblePids.filter(pid -> {
      Path cmdline = Paths.get("/proc", pid.toString(), "cmdline");
      // We can skip "self" as its just a symlink
      try {
        if (!Files.exists(cmdline)) {
          return false;
        }
        String s = new String(Files.readAllBytes(cmdline), Charset.defaultCharset());
        return s.toLowerCase().contains(clazz.getCanonicalName().toLowerCase());
      } catch (IOException e) {
        LOG.debug("Failed to read {} for comm", cmdline, e);
      }
      return false;
    }).collect(Collectors.toList());
  }

  @Override
  public boolean isProcessAlive(int pid) {
    Path p = Paths.get("/proc", Integer.toString(pid));
    return Files.exists(p);
  }
}
