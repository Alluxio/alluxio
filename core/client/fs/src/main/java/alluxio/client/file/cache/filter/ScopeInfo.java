package alluxio.client.file.cache.filter;

import java.util.Objects;

public class ScopeInfo {
    private String scope;

    public ScopeInfo(String scope) {
        this.scope = scope;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScopeInfo scopeInfo = (ScopeInfo) o;
        return Objects.equals(scope, scopeInfo.scope);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope);
    }

    @Override
    public String toString() {
        return "ScopeInfo{" +
                "scope='" + scope + '\'' +
                '}';
    }
}
