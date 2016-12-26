package mongo.subscription;

import org.junit.Before;
import org.junit.Test;

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
                .setSourceALiases("1", "2", "3", "4");
    }

    @Test
    public void testInserts() throws InterruptedException {
        Subscription subscription = new Subscription(configuration, System.out::println);
        subscription.prepare();
        new DataProducer(configuration).insertData();
        subscription.read();
    }

}
