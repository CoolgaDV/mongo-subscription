package mongo.subscription;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Dmitry Coolga
 *         25.12.2016 10:20
 */
public class SubscriptionTest {

    private Configuration configuration;

    @Before
    public void setUp() {

        Logger.getLogger("org.mongodb").setLevel(Level.SEVERE);

        configuration = new Configuration()
                .setDatabaseHost("localhost")
                .setDatabasePort(27017)
                .setDatabaseName("subscription")
                .setMessagesCount(25)
                .setSourceALiases("1", "2", "3", "4");
    }

    @Test
    public void testInserts() throws InterruptedException {

        CountDownLatch latch = new CountDownLatch(
                configuration.getSourceAliases().size() + 1);

        try (Subscription subscription = new Subscription(configuration, latch);
             DataProducer producer = new DataProducer(configuration, latch)
        ) {
            subscription.prepare();
            producer.insertData();
            subscription.listen();
            Assert.assertTrue(latch.await(1, TimeUnit.MINUTES));
        }
    }

}
