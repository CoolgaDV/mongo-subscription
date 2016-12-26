package mongo.subscription;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Dmitry Coolga
 *         25.12.2016 10:16
 */
public class DataProducer {

    private final Configuration configuration;

    public DataProducer(Configuration configuration) {
        this.configuration = configuration;
    }

    public void insertData() throws InterruptedException {
        List<String> sourcesAliases = configuration.getSourceAliases();
        CountDownLatch latch = new CountDownLatch(sourcesAliases.size());
        MongoContext.execute(configuration, context -> {
            for (String sourceAlias : sourcesAliases) {
                new Thread(() -> {
                    DBObject sequence = context.getSequenceCollection().findOneAndUpdate(
                            new BasicDBObject("_id", "id"),
                            new BasicDBObject("$inc", new BasicDBObject("seq", 1)),
                            new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
                    context.getDataCollection().insertOne(new BasicDBObject()
                            .append("seq", sequence.get("seq"))
                            .append("source", sourceAlias)
                            .append("ts", new SimpleDateFormat("HH:mm:ss.SSS").format(new Date())));
                    latch.countDown();
                }).start();
            }
            try {
                latch.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) { }
        });
    }

}
