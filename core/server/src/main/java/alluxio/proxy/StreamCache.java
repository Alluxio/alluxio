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

package alluxio.proxy;

import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * File stream cache.
 */
// TODO(jiri): Add support for evicting stale streams.
public final class StreamCache {
  private static Map<Integer, FileOutStream> sOutStreams = new ConcurrentHashMap<>();
  private static Map<Integer, FileInStream> sInStreams = new ConcurrentHashMap<>();
  private static AtomicInteger sCounter = new AtomicInteger();

  public static FileInStream getInStream(Integer id) {
    return sInStreams.get(id);
  }

  public static FileOutStream getOutStream(Integer id) {
    return sOutStreams.get(id);
  }

  public static Integer put(FileInStream is) {
    int id = sCounter.incrementAndGet();
    sInStreams.put(id, is);
    return id;
  }

  public static Integer put(FileOutStream os) {
    int id = sCounter.incrementAndGet();
    sOutStreams.put(id, os);
    return id;
  }

  public static Closeable remove(Integer id) {
    FileInStream is = sInStreams.remove(id);
    if (is != null) {
      return is;
    }
    FileOutStream os = sOutStreams.remove(id);
    if (os != null) {
      return os;
    }
    return null;
  }
}
