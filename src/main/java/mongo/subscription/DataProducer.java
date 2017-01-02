package mongo.subscription;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Coolga
 *         25.12.2016 10:16
 */
public class DataProducer implements AutoCloseable {

    private final Configuration configuration;
    private final CountDownLatch latch;
    private final ExecutorService executor;

    public DataProducer(Configuration configuration, CountDownLatch latch) {
        this.configuration = configuration;
        this.latch = latch;
        executor = Executors.newFixedThreadPool(configuration.getSourceAliases().size());
    }

    public void insertData() throws InterruptedException {
        for (String sourceAlias : configuration.getSourceAliases()) {
            executor.execute(() -> MongoContext.execute(
                    configuration,
                    context -> insert(context, sourceAlias)));
        }
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private void insert(MongoContext context, String sourceAlias) {
        for (int index = 1; index <= configuration.getMessagesCount(); index++) {
            DBObject sequence = context.getSequenceCollection().findOneAndUpdate(
                    new BasicDBObject("_id", "id"),
                    new BasicDBObject("$inc", new BasicDBObject("seq", 1)),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
            context.getDataCollection().insertOne(new BasicDBObject()
                    .append("seq", sequence.get("seq"))
                    .append("source", sourceAlias)
                    .append("message", sourceAlias + "-" + index)
                    .append("ts", new SimpleDateFormat("HH:mm:ss.SSS").format(new Date())));
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ex) {
                if (executor.isShutdown()) {
                    return;
                }
            }
        }
        latch.countDown();
    }

}
