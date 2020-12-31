package hybrid.rdf.storage.config;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import lombok.Data;
import org.apache.jena.rdf.model.Resource;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.mongodb.MongoClientSettings;

import java.util.HashMap;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
@ConfigurationProperties(prefix = "hybrid.rdf.storage.mongo")
@Data
public class MongodbConfig
{
    private String uri;

    private String db;

    private String collection;

    @Bean
    public MongoCollection<Document> getMongoCollection()
    {
        ConnectionString connString = new ConnectionString(uri);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connString)
                .retryWrites(true)
                .build();

        MongoClient mongoClient = MongoClients.create(settings);

        CodecRegistry pojoCodecRegistry = fromRegistries(com.mongodb.MongoClient.getDefaultCodecRegistry(),
                                                         fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        return mongoClient
                .getDatabase(db)
                .getCollection(collection, Document.class)
                .withCodecRegistry(pojoCodecRegistry);
    }

    public static class RdfPropertyMap extends HashMap<String, Object> {
        public RdfPropertyMap(Resource resource) {
            super();
            this.put("resource", resource.getURI());
        }
    }
}
