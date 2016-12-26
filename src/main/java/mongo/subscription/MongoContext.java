package mongo.subscription;

import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.util.function.Consumer;

/**
 * @author Dmitry Coolga
 *         25.12.2016 10:31
 */
public class MongoContext {

    private final Configuration configuration;

    private MongoCollection<DBObject> sequenceCollection;
    private MongoCollection<DBObject> dataCollection;

    private MongoContext(Configuration configuration) {
        this.configuration = configuration;
    }

    public static void execute(Configuration configuration,
                               Consumer<MongoContext> code) {
        new MongoContext(configuration).execute(code);
    }

    public MongoCollection<DBObject> getSequenceCollection() {
        return sequenceCollection;
    }

    public MongoCollection<DBObject> getDataCollection() {
        return dataCollection;
    }

    private void execute(Consumer<MongoContext> code) {
        String host = configuration.getDatabaseHost();
        int port = configuration.getDatabasePort();
        try (MongoClient client = new MongoClient(host, port)) {
            MongoDatabase db = client.getDatabase(configuration.getDatabaseName());
            sequenceCollection = db.getCollection("sequence", DBObject.class);
            dataCollection = db.getCollection("data", DBObject.class);
            code.accept(this);
        }
    }

}
