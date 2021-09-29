package alluxio.client.file.cache.filter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ScopeEncoder {
    private final int bitsPerScope;
    private final int maxNumScopes;
    private int count; // the next scope id
    private final ConcurrentHashMap<ScopeInfo, Integer> scopeToId;
    private final ConcurrentHashMap<Integer, ScopeInfo> idToScope;
    private final Lock lock;

    public ScopeEncoder(int bitsPerScope) {
        this.bitsPerScope = bitsPerScope;
        this.maxNumScopes = (1 << bitsPerScope);
        this.count = 0;
        this.scopeToId = new ConcurrentHashMap<>();
        this.idToScope = new ConcurrentHashMap<>();
        this.lock = new ReentrantLock();
    }

    public int encode(ScopeInfo scopeInfo) {
        if (!scopeToId.containsKey(scopeInfo)) {
            lock.lock();
            if (!scopeToId.containsKey(scopeInfo)) {
                scopeToId.put(scopeInfo, count);
                idToScope.put(count, scopeInfo);
                count++;
            }
            lock.unlock();
        }
        return scopeToId.get(scopeInfo);
    }

    public ScopeInfo decode(int id) {
        return idToScope.get(id);
    }
}
