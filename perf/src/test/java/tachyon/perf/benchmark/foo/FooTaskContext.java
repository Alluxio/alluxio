package tachyon.perf.benchmark.foo;

import java.io.File;
import java.io.IOException;

import tachyon.perf.basic.PerfTaskContext;
import tachyon.perf.basic.PerfThread;

public class FooTaskContext extends PerfTaskContext {
  private int mFoo = 0;
  private boolean mLoad = false;
  private boolean mThreads = false;
  private boolean mWrite = false;

  @Override
  public void loadFromFile(File file) throws IOException {
    mLoad = true;
  }

  @Override
  public void setFromThread(PerfThread[] threads) {
    mThreads = true;
    for (PerfThread thread : threads) {
      mThreads &= ((FooThread) thread).getSetup();
      mThreads &= ((FooThread) thread).getRun();
      mThreads &= ((FooThread) thread).getCleanup();
    }
  }

  @Override
  public void writeToFile(File file) throws IOException {
    mWrite = true;
  }

  public int getFoo() {
    return mFoo;
  }

  public boolean getLoad() {
    return mLoad;
  }

  public boolean getThreads() {
    return mThreads;
  }

  public boolean getWrite() {
    return mWrite;
  }

  public void setFoo(int foo) {
    mFoo = foo;
  }
}
