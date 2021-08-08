package hybrid.rdf.storage;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoReactiveAutoConfiguration;
import org.springframework.stereotype.Repository;

@SpringBootApplication(
        scanBasePackages = {"hybrid.rdf.storage"},
        exclude = {MongoAutoConfiguration.class, MongoReactiveAutoConfiguration.class}
)
@MapperScan(
        basePackages = "hybrid.rdf.storage.*",
        sqlSessionTemplateRef = "sqlSessionTemplate",
        annotationClass = Repository.class
)
public class  App  {
    public static void main(String[] args)
    {
        SpringApplication.run(App.class, args);
    }
}

