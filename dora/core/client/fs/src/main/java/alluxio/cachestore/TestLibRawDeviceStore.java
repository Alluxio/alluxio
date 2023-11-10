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

package alluxio.cachestore;

import alluxio.cachestore.utils.NativeLibraryLoader;

import java.nio.ByteBuffer;

public class TestLibRawDeviceStore {

  public static void main(String [] argv) {
    LibRawDeviceStore.loadLibrary(NativeLibraryLoader.LibCacheStoreVersion.VERSION_1);
    LibRawDeviceStore libRawDeviceStore = new LibRawDeviceStore();
    libRawDeviceStore.openCacheStore("/home/ec2-user/SourceCode/ceph/src/test/fio/ceph-bluestore.conf");
    System.out.println(String.format("Successfully open the cachestore"));
    ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
    byteBuffer.putChar('f');
    byteBuffer.putChar('b');
    byteBuffer.putChar('c');
    byteBuffer.putChar('d');
    LibRawDeviceStore.ReturnStatus returnStatus
        = libRawDeviceStore.putPage("abcd", 0, byteBuffer, 4, false);
    System.out.println(String.format("Return value is : %d", returnStatus));
    byteBuffer.clear();
    int size = libRawDeviceStore.getPage("abcd", 0, 0, 3,
        byteBuffer,false);

    String str = new String();
    for (int i = 0; i < 1; i++) {
      char achar = byteBuffer.getChar(i);
      str = String.valueOf(achar);
    }
    System.out.println(String.format("Read size is %d, content:%s", size, str));

    LibRawDeviceStore.JniPageInfo[] jniPageInfos = libRawDeviceStore
        .listPages("", 0, 100);
    for (LibRawDeviceStore.JniPageInfo pageInfo : jniPageInfos) {
      System.out.println(String.format("Found %s %d %d %d", pageInfo.getFileId(),
          pageInfo.getPageId(), pageInfo.getFileSize(), pageInfo.getCreationTime()));
    }


    ByteBuffer nonDirectBuffer = ByteBuffer.allocate(1024);
    nonDirectBuffer.putChar('f');
    nonDirectBuffer.putChar('b');
    nonDirectBuffer.putChar('c');
    nonDirectBuffer.putChar('d');
    LibRawDeviceStore.ReturnStatus ret
        = libRawDeviceStore.putPageByteArray("efg", 0, nonDirectBuffer.array(),
        4, false);
  }
}
