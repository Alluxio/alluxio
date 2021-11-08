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

import alluxio.collections.Pair;
import alluxio.util.LogUtils;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementation of the {@link ProcessTable} for MacOS-based systems.
 */
public class MacOsProcessTable implements ProcessTable {
  private static final Logger LOG = LoggerFactory.getLogger(MacOsProcessTable.class);

  @Override
  public Stream<Integer> getJavaPids() throws IOException {
    return getPsOutput().entrySet().stream()
        .filter(e -> e.getValue().contains("java"))
        .map(Map.Entry::getKey);
  }

  @Override
  public Stream<Integer> getAllPids() throws IOException {
    return getPsOutput().keySet().stream();
  }

  @Override
  public List<Integer> getJavaPid(@Nullable Stream<Integer> possiblePids, Class<?> clazz)
      throws IOException {
    Map<Integer, String> psOutput = getPsOutput();
    if (possiblePids == null) {
      possiblePids = getPsOutput().keySet().stream();
    }
    return possiblePids.filter(pid -> {
      String output = psOutput.get(pid);
      if (output == null) {
        return false;
      }
      return output.toLowerCase().contains(clazz.getCanonicalName().toLowerCase());
    }).collect(Collectors.toList());
  }

  @Override
  public boolean isProcessAlive(int pid) {
    try {
      return getPsOutput().containsKey(pid);
    } catch (IOException e) {
      LogUtils.warnWithException(LOG, "Failed to get ps output when checking if {} is alive",
          pid, e);
      return false;
    }
  }

  Map<Integer, String> getPsOutput() throws IOException {
    Process p = Runtime.getRuntime().exec("ps -e -o pid,command");
    // in case there is more stdout, we should read it to make sure that the buffers aren't filled
    BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
    BufferedReader outReader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    try {
      List<String> output = outReader.lines().collect(Collectors.toList());
      if (p.waitFor() != 0) {
        throw new IOException("Failed to gather process table output");
      }
      // skip first line for header
      return output.stream().skip(1).map(line -> {
        String[] items = line.trim().split(" ", 2);
        return new Pair<>(items[0], items[1]);
      }).collect(Collectors.toMap(i -> Integer.parseInt(i.getFirst()), Pair::getSecond));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while waiting to read process table");
    } finally {
      try {
        outReader.close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stdout reader when reading process table", e);
      }
      try {
        errReader.close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stderr reader when reading process table", e);
      }
      try {
        p.getOutputStream().close();
      } catch (IOException e) {
        LOG.debug("Failed to properly close stdin stream when reading process table", e);
      }
    }
  }
}
