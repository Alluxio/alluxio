package alluxio.master.file.metasync;

import alluxio.master.file.meta.LockedInodePath;
import alluxio.underfs.UfsStatus;

import java.util.Iterator;
import java.util.stream.Stream;

class UfsStatusIterator implements Iterator<UfsStatus> {

  UfsStatusIterator(Stream<UfsStatus> ufsStream,
                    LockedInodePath mountLockedPath) {

  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public UfsStatus next() {
    return null;
  }
}
