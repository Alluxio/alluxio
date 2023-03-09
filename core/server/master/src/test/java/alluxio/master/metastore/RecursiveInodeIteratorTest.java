package alluxio.master.metastore;

import static org.junit.Assert.*;
import alluxio.master.file.contexts.CreateDirectoryContext;
import alluxio.master.file.contexts.CreateFileContext;
import alluxio.master.file.meta.MutableInodeDirectory;
import alluxio.master.file.meta.MutableInodeFile;

public class RecursiveInodeIteratorTest {
  private static MutableInodeDirectory inodeDir(long id, long parentId, String name) {
    return MutableInodeDirectory.create(id, parentId, name, CreateDirectoryContext.defaults());
  }

  private static MutableInodeFile inodeFile(long containerId, long parentId, String name) {
    return MutableInodeFile.create(containerId, parentId, name, 0, CreateFileContext.defaults());
  }


}