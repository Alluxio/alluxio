package tachyon.client;

import tachyon.client.file.options.InStreamOptions;
import tachyon.client.file.options.OutStreamOptions;
import tachyon.conf.TachyonConf;
import tachyon.util.network.NetworkAddressUtils;

public class StreamOptionUtils {
  private StreamOptionUtils() {
    // not intended for instantiation
  }

  public static OutStreamOptions getOutStreamOptionsWriteBoth(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
               .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
  }

  public static OutStreamOptions getOutStreamOptionsWriteTachyon(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
               .setUnderStorageType(UnderStorageType.NO_PERSIST).build();
  }

  public static OutStreamOptions getOutStreamOptionsWriteUnderStore(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.NO_STORE)
               .setUnderStorageType(UnderStorageType.SYNC_PERSIST).build();
  }

  public static OutStreamOptions getOutStreamOptionsWriteLocal(TachyonConf conf) {
    return new OutStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
               .setUnderStorageType(UnderStorageType.SYNC_PERSIST)
               .setHostname(NetworkAddressUtils.getLocalHostName(conf)).build();
  }

  public static InStreamOptions getInStreamOptionsReadCache(TachyonConf conf) {
    return new InStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.STORE)
        .build();
  }

  public static InStreamOptions getInStreamOptionsReadNoCache(TachyonConf conf) {
    return new InStreamOptions.Builder(conf).setTachyonStorageType(TachyonStorageType.NO_STORE)
        .build();
  }

}
