package mongo.subscription;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Dmitry Coolga
 *         25.12.2016 10:16
 */
public class Configuration {

    private String databaseHost;
    private int databasePort;
    private String databaseName;

    private final List<String> sourceAliases = new ArrayList<>();

    public Configuration setDatabaseHost(String databaseHost) {
        this.databaseHost = databaseHost;
        return this;
    }

    public Configuration setDatabasePort(int databasePort) {
        this.databasePort = databasePort;
        return this;
    }

    public Configuration setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    public Configuration setSourceALiases(String... sourceALiases) {
        this.sourceAliases.clear();
        Arrays.stream(sourceALiases).forEach(this.sourceAliases::add);
        return this;
    }

    public String getDatabaseHost() {
        return databaseHost;
    }

    public int getDatabasePort() {
        return databasePort;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public List<String> getSourceAliases() {
        return sourceAliases;
    }

}
