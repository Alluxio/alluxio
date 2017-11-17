package alluxio.collections;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.commons.lang.Validate;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Prefix list is used to do file filtering.
 */
@ThreadSafe
public final class PrefixMap {
    private final ImmutableSortedMap<String, String> mInnerMap;

    /**
     * Prefix map is used to do file filtering and return the corresponding value
     *
     * @param prefixMap the map of prefixes to create
     */
    public PrefixMap(Map<String, String> prefixMap) {
        if (prefixMap.isEmpty()) {
            mInnerMap = ImmutableSortedMap.of();
        } else {
            mInnerMap = ImmutableSortedMap.copyOf(prefixMap);
        }
    }

    public PrefixMap (List<String> prefixkvs, String kvSeparator) {
        Validate.notNull(prefixkvs);
        Validate.notNull(kvSeparator);
        Map<String, String> prefixMap = new TreeMap<>();
        for (String prefixkv : prefixkvs) {
            if (prefixkv != null && !prefixkv.trim().isEmpty()) {
                String[] kv = prefixkv.trim().split(kvSeparator);
                if (kv.length > 1) {
                    prefixMap.put(kv[0], kv[1]);
                }
            }
        }
        mInnerMap = ImmutableSortedMap.copyOf(prefixMap);
    }

    public PrefixMap (String prefixes, String entrySeparator, String kvSeparator) {
        Validate.notNull(entrySeparator);
        Validate.notNull(kvSeparator);
        Map<String, String> prefixMap = new TreeMap<>();
        if (prefixes != null && !prefixes.trim().isEmpty()) {
            String[] kvEntries = prefixes.trim().split(entrySeparator);
            for (String kvEntry : kvEntries) {
                if (kvEntry != null && !kvEntry.trim().isEmpty()) {
                    String[] kv = kvEntry.trim().split(kvSeparator);
                    if (kv.length > 1) {
                        prefixMap.put(kv[0], kv[1]);
                    }
                }
            }
        }
        mInnerMap = ImmutableSortedMap.copyOf(prefixMap);
    }

    /**
     * Gets the list of prefixes.
     *
     * @return the list of prefixes
     */
    public ImmutableSortedMap getList() {
        return mInnerMap;
    }

    /**
     * Checks whether a prefix of {@code path} is in the prefix map.
     *
     * @param path the path to check
     * @return the corresponding value if the path is in the map, empty value otherwise.
     */
    public String inMap(String path) {
        String value = "";
        if (!Strings.isNullOrEmpty(path)) {
            for (String prefix : mInnerMap.keySet()) {
                if (path.startsWith(prefix)) {
                    value = mInnerMap.get(prefix);
                }
            }
        }
        return value;
    }

    /**
     * Checks whether a prefix of {@code path} is not in the prefix map.
     *
     * @param path the path to check
     * @return true if the path is not in the list, false otherwise
     */
    public boolean outList(String path) {
        return inMap(path).isEmpty();
    }

    /**
     * Print out all kv pairs separated by ";" and kv are separated by ":".
     *
     * @return the kv representation like "key1:value1;key2:value2;"
     */
    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<String, String> entry : mInnerMap.entrySet()) {
            s.append(entry.getKey()).append(":").append(entry.getValue()).append(";");
        }
        return s.toString();
    }
}
