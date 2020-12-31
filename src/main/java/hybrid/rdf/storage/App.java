package hybrid.rdf.storage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration;

@SpringBootApplication(
        scanBasePackages = {"hybrid.rdf.storage"},
        exclude = {MongoAutoConfiguration.class, MongoReactiveAutoConfiguration.class}
)
public class App  {
    public static void main(String[] args)
    {
        SpringApplication.run(App.class, args);
    }
}

