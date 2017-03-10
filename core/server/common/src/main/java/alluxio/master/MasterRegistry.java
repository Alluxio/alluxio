package alluxio.master;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MasterRegistry {
  private final Map<String, Master> mRegistry = new HashMap<>();

  public MasterRegistry() {}

  public synchronized <T> T get(String name, Class<T> type) {
    Master master = mRegistry.get(name);
    if (master == null || !(type.isInstance(master))) {
      return null;
    }
    return (T) master;
  }

  public synchronized void put(String name, Master master) {
    mRegistry.put(name, master);
  }

  // TODO(jiri): better iteration
  public synchronized Collection<Master> getMasters() {
    return mRegistry.values();
  }

  public class Value<T> {
    private Class<T> mClass;
    private T mMaster;
    private String mName;

    public Value(String name, Class clazz) {
      mName = name;
      mClass = clazz;
    }

    public T get() {
      if (mMaster == null) {
        synchronized (this) {
          if (mMaster == null) {
            mMaster = MasterRegistry.this.get(mName, mClass);
          }
        }
      }
      return mMaster;
    }
  }
}
