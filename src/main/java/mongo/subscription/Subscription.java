package mongo.subscription;

import com.mongodb.DBObject;
import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Dmitry Coolga
 *         25.12.2016 10:15
 */
public class Subscription {

    private final Configuration configuration;
    private final Consumer<DBObject> listener;
    private final Map<String, Long> limits;

    public Subscription(Configuration configuration, Consumer<DBObject> listener) {
        this.configuration = configuration;
        this.listener = listener;
        this.limits = configuration.getSourceAliases()
                .stream()
                .collect(Collectors.toMap(Function.identity(), alias -> 0L));
    }

    public void prepare() {
        query(item -> { });
    }

    public void read() {
        query(listener);
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
