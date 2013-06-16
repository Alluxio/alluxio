package tachyon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.esotericsoftware.kryo.Kryo;

import tachyon.thrift.NetAddress;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;

/**
 * The factory to create a kryo ser/de for Tachyon master write ahead log or checkpoint file.
 */
public class KryoFactory {
  public static Kryo createLogKryo() {
    Kryo ret = new KryoReflectionFactorySupport();
    ret.register(HashSet.class);
    ret.register(HashMap.class);
    ret.register(ArrayList.class);
    ret.register(LogType.class);
    ret.register(InodeType.class);
    ret.register(NetAddress.class);
    ret.register(BlockInfo.class);
    ret.register(InodeFile.class);
    ret.register(InodeFolder.class);
    ret.register(InodeRawTable.class);
    ret.register(CheckpointInfo.class);

    return ret;
  }
}