package hybrid.rdf.storage.config;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Neo4JClientConfig {

    @Bean
    public Graph getGraph() {
        Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "1"));
        Neo4JElementIdProvider<Long> provider = new Neo4JNativeElementIdProvider();
        return new Neo4JGraph(driver, "neo4j", provider, provider);
    }
}
