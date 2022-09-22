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

package alluxio.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * FileUtil for TarUtilsTest and ParallelZipUtilsTest.
 */
public class FileUtil {
  static void assertDirectoriesEqual(Path path, Path reconstructed) throws Exception {
    Files.walk(path).forEach(subPath -> {
      Path relative = path.relativize(subPath);
      Path resolved = reconstructed.resolve(relative);
      assertTrue(resolved + " should exist since " + subPath + " exists", Files.exists(resolved));
      assertEquals(subPath.toFile().isFile(), resolved.toFile().isFile());
      if (path.toFile().isFile()) {
        try {
          assertArrayEquals(resolved + " should have the same content as " + subPath,
              Files.readAllBytes(path), Files.readAllBytes(resolved));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
