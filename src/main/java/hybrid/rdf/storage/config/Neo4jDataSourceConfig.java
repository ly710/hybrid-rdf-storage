package hybrid.rdf.storage.config;

import org.neo4j.driver.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Neo4jDataSourceConfig {
    @Bean
    public CypherExecutor getCypherExecutor() {
        Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1"));
        return new CypherExecutor(driver);
    }

    public static class CypherExecutor {
        private final Driver driver;

        public CypherExecutor(Driver driver) {
            this.driver = driver;
        }

        public void execute(String cypher) {
            Session session = driver.session();
            Transaction tx = session.beginTransaction();
            tx.run(cypher);
            tx.commit();
            tx.close();
        }
    }
}
