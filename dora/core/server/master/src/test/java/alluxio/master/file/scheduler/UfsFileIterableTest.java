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

package alluxio.master.file.scheduler;

import alluxio.Constants;
import alluxio.conf.Configuration;
import alluxio.master.job.UfsFileIterable;
import alluxio.master.predicate.FilePredicate;
import alluxio.proto.journal.Job;
import alluxio.underfs.UnderFileSystem;
import alluxio.wire.FileInfo;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Optional;
import java.util.function.Predicate;

public class UfsFileIterableTest {
  @Rule
  public TemporaryFolder mTestFolder = new TemporaryFolder();

  @Test
  public void testLocalUfs() throws IOException {
    File root = mTestFolder.newFolder("root");
    File file = mTestFolder.newFile("root/test");
    UnderFileSystem ufs =
        UnderFileSystem.Factory.create(root.getAbsolutePath(), Configuration.global());
    UfsFileIterable ufsFileIterable =
        new UfsFileIterable(ufs, root.getAbsolutePath(), Optional.empty(), FileInfo::isCompleted);
    Iterator<FileInfo> iterator = ufsFileIterable.iterator();
    FileInfo fileInfo = iterator.next();
    Assert.assertEquals(file.getAbsolutePath(), fileInfo.getPath());
    Assert.assertEquals(file.lastModified(), fileInfo.getLastModificationTimeMs());
    Assert.assertFalse(iterator.hasNext()); // local file system doesn't include directory status
  }

  @Test
  public void testDatePredicate() throws IOException {
    File root = mTestFolder.newFolder("root");
    File file = mTestFolder.newFile("root/test");
    UnderFileSystem ufs =
        UnderFileSystem.Factory.create(root.getAbsolutePath(), Configuration.global());
    Date date = new Date(file.lastModified());
    date.setTime(date.getTime() + Constants.DAY);
    String filterValue = String.format("lastModifiedDate(2000/01/01, %s)",
        new SimpleDateFormat("yyyy/MM/dd").format(date));
    int pos = filterValue.indexOf("(");
    String filterName = filterValue.substring(0, pos);
    String value = filterValue.substring(pos + 1, filterValue.length() - 1);
    Job.FileFilter.Builder builder =
        Job.FileFilter.newBuilder().setName(filterName).setValue(value);
    Predicate<FileInfo> predicate = FilePredicate.create(builder.build()).get();
    UfsFileIterable ufsFileIterable =
        new UfsFileIterable(ufs, root.getAbsolutePath(), Optional.empty(), predicate);
    Iterator<FileInfo> iterator = ufsFileIterable.iterator();
    FileInfo fileInfo = iterator.next();
    Assert.assertEquals(file.getAbsolutePath(), fileInfo.getPath());
    Assert.assertEquals(file.lastModified(), fileInfo.getLastModificationTimeMs());
  }

  @Test
  public void testDatePredicateNotQualified() throws IOException {
    File root = mTestFolder.newFolder("root");
    File file = mTestFolder.newFile("root/test");
    UnderFileSystem ufs =
        UnderFileSystem.Factory.create(root.getAbsolutePath(), Configuration.global());
    Date date = new Date(file.lastModified());
    // this predicate is left inclusive right exclusive so this should not filter out the file
    String filterValue = String.format("lastModifiedDate(2000/01/01, %s)",
        new SimpleDateFormat("yyyy/MM/dd").format(date));
    int pos = filterValue.indexOf("(");
    String filterName = filterValue.substring(0, pos);
    String value = filterValue.substring(pos + 1, filterValue.length() - 1);
    Job.FileFilter.Builder builder =
        Job.FileFilter.newBuilder().setName(filterName).setValue(value);
    Predicate<FileInfo> predicate = FilePredicate.create(builder.build()).get();
    UfsFileIterable ufsFileIterable =
        new UfsFileIterable(ufs, root.getAbsolutePath(), Optional.empty(), predicate);
    Iterator<FileInfo> iterator = ufsFileIterable.iterator();
    Assert.assertFalse(iterator.hasNext());
  }
}
