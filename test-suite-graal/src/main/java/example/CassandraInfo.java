package example;

import io.micronaut.core.annotation.Introspected;

@Introspected
public class CassandraInfo {
    private final String clusterName;
    private final String releaseVersion;

    public CassandraInfo(String clusterName, String releaseVersion) {
        this.clusterName = clusterName;
        this.releaseVersion = releaseVersion;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getReleaseVersion() {
        return releaseVersion;
    }
}