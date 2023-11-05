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

package alluxio.proxy.s3;

import alluxio.AlluxioURI;
import alluxio.client.file.URIStatus;
import alluxio.s3.ListPrefixIterator;
import alluxio.s3.ListPrefixIterator.ChildrenSupplier;
import alluxio.wire.FileInfo;
import com.google.common.collect.Streams;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ListPrefixIteratorTest {

  @Rule
  public final TemporaryFolder mTestFolder = new TemporaryFolder();

  private final Random mRandom = new Random();
  private final char[] mChars = new char[]{'a', 'b', 'c'};
  private final ChildrenSupplier mLocalFileChildrenSupplier = alluxioURI -> {
    File[] files = new File(alluxioURI.getPath()).listFiles();
    if (files == null) {
      return Collections.emptyList();
    } else {
      return Arrays.stream(files).map(this::fromFile).collect(Collectors.toList());
    }
  };

  private String getRandomFileName() {
    int length = mRandom.nextInt(3) + 1;
    StringBuilder name = new StringBuilder();
    for (int i = 0; i < length; i++) {
      name.append(mChars[mRandom.nextInt(mChars.length)]);
    }
    return name.toString();
  }

  private String getRandomFilePath(int depths) {
    return IntStream.range(0, depths)
        .mapToObj(i -> getRandomFileName())
        .collect(Collectors.joining("/"));
  }

  public List<String> createRandomTmpFile(String root) throws IOException {
    // clear
    mTestFolder.delete();
    // create 100 dirs
    int depths = mRandom.nextInt(3) + 1;
    Set<String> files = new HashSet<>();
    for (int i = 0; i < 100; i++) {
      files.add(getRandomFilePath(depths));
    }
    return files.stream().sorted(Comparator.naturalOrder()).map(s -> {
          try {
            return mTestFolder.newFolder(root + File.separator + s).getPath();
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        })
        .collect(Collectors.toList());
  }

  @Test
  public void testUriStatusIteratorWithoutPrefix() throws IOException {
    for (int i = 0; i < 100; i++) {
      String rootDir = "root" + i;
      createRandomTmpFile(rootDir);
      AlluxioURI path = new AlluxioURI(mTestFolder.getRoot().getPath() + File.separator + rootDir);
      ListPrefixIterator listPrefixIterator = new ListPrefixIterator(path, mLocalFileChildrenSupplier,
          null);
      List<String> actual = Streams.stream(listPrefixIterator).map(URIStatus::getPath)
          .collect(Collectors.toList());
      List<String> expect = listLocalDir(
          new AlluxioURI(mTestFolder.getRoot().getPath() + File.separator + rootDir));
      Assert.assertEquals(expect, actual);
    }
  }

  @Test
  public void testUriStatusIteratorWithPrefix() throws IOException {
    for (int i = 0; i < 100; i++) {
      String rootDir = "root" + i;
      createRandomTmpFile(rootDir);
      AlluxioURI path = new AlluxioURI(mTestFolder.getRoot().getPath() + File.separator + rootDir);
      List<String> local = listLocalDir(
          new AlluxioURI(mTestFolder.getRoot().getPath() + File.separator + rootDir));
      // test 100 times: random select prefix
      for (int j = 0; j < 100; j++) {
        String prefix = local.get(mRandom.nextInt(local.size()));
        ListPrefixIterator listPrefixIterator = new ListPrefixIterator(path,
            mLocalFileChildrenSupplier, prefix);
        List<String> actual = Streams.stream(listPrefixIterator).map(URIStatus::getPath)
            .collect(Collectors.toList());
        List<String> expect = local.stream().filter(s -> s.startsWith(prefix))
            .collect(Collectors.toList());
        Assert.assertEquals(expect, actual);
      }
    }
  }

  private URIStatus fromFile(File file) {
    return new URIStatus(new FileInfo().setPath(file.getPath()).setFolder(file.isDirectory()));
  }

  private List<String> listLocalDir(AlluxioURI path) throws IOException {
    return Files.walk(new File(path.getPath()).toPath(),
            FileVisitOption.FOLLOW_LINKS)
        .map(Path::toString)
        .sorted(Comparator.naturalOrder())
        .skip(1)
        .collect(Collectors.toList());
  }

}