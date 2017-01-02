package mongo.subscription;

import com.mongodb.DBObject;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author Dmitry Coolga
 *         25.12.2016 10:15
 */
public class Subscription implements AutoCloseable {

    private final Configuration configuration;
    private final CountDownLatch latch;

    private final Map<String, Long> limits;
    private final Set<String> expectedMessages;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public Subscription(Configuration configuration, CountDownLatch latch) {
        this.configuration = configuration;
        this.latch = latch;
        this.limits = configuration.getSourceAliases()
                .stream()
                .collect(Collectors.toMap(Function.identity(), alias -> 0L));
        this.expectedMessages = IntStream.range(1, configuration.getMessagesCount() + 1)
                .mapToObj(Integer::toString)
                .flatMap(value -> configuration.getSourceAliases()
                        .stream()
                        .map(alias -> alias + "-" + value))
                .collect(Collectors.toSet());
    }

    public void prepare() {
        query(item -> { });
    }

    public void listen() {
        executor.execute(() -> {
            while (true) {
                query(item -> {
                    String message = (String) item.get("message");
                    expectedMessages.remove(message);
                });
                if (expectedMessages.isEmpty()) {
                    latch.countDown();
                    return;
                }
                try {
                    TimeUnit.MILLISECONDS.sleep(50);
                } catch (InterruptedException ex) {
                    if (executor.isShutdown()) {
                        return;
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private void query(Consumer<DBObject> handler) {
        MongoContext.execute(configuration, context -> {
            Bson query = Filters.or(limits.entrySet().stream()
                    .map(entry -> Filters.and(
                            Filters.eq("source", entry.getKey()),
                            Filters.gt("seq", entry.getValue())))
                    .toArray(Bson[]::new));
            context.getDataCollection().find(query).forEach((Consumer<DBObject>) (item) -> {
                String source = (String) item.get("source");
                long sequence = ((Double) item.get("seq")).longValue();
                if (limits.get(source) < sequence) {
                    limits.put(source, sequence);
                }
                handler.accept(item);
            });
        });
    }

}
