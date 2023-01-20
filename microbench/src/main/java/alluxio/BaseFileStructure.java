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

package alluxio;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.generator.ZipfianGenerator;

import java.util.ArrayList;

/**
 * Governs the file structure parameters inside a JMH micro bench.
 */
@State(Scope.Benchmark)
public class BaseFileStructure {
  // each depth level needs its own file id generator
  public ArrayList<NumberGenerator> mFileGenerators;
  public NumberGenerator mDepthGenerator;
  public NumberGenerator mWidthGenerator;

  public enum Distribution { UNIFORM, ZIPF }

  public void init(int depth, int width, int fileCount, Distribution distribution) {
    mFileGenerators = new ArrayList<>();
    switch (distribution) {
      case ZIPF:
        mDepthGenerator = new ZipfianGenerator(0, depth);
        mWidthGenerator = new ZipfianGenerator(0, width);
        for (int i = 0; i < depth + 1; i++) {
          mFileGenerators.add(new ZipfianGenerator(0, fileCount - 1));
        }
        break;
      default:
        mDepthGenerator = new UniformLongGenerator(0, depth);
        mWidthGenerator = new UniformLongGenerator(0, width);
        for (int i = 0; i < depth + 1; i++) {
          mFileGenerators.add(new UniformLongGenerator(0, fileCount - 1));
        }
    }
  }
}
