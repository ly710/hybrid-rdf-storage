package hybrid.rdf.storage.client;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JVertex;

import java.util.*;

public class ResultList extends ArrayList<Map<String, ?>> {
    public static class Mappings {
        private final List<String> vars;

        private final Object mappings;

        public Mappings(List<String> vars, Object mappings) {
            this.vars = vars;
            this.mappings = mappings;
        }

        public Map<String, ?> getMappings() {
            Map<String, Object> realMappings = new HashMap<>();

            if(Objects.equals(vars.size(), 1)) {
                realMappings.put(vars.get(0), getSingleMapping(mappings));
            } else {
                if(mappings instanceof LinkedHashMap) {
                    for (String var : vars)
                    {
                        realMappings.put(var, getSingleMapping(((LinkedHashMap) mappings).get(var)));
                    }
                }
            }

            return realMappings;
        }

        private Object  getSingleMapping(Object mapping) {
            if(mapping instanceof Neo4JVertex) {
                Neo4JVertex vertex = (Neo4JVertex)mapping;
                return vertex.property("name").value().toString();
            }

            return mapping;
        }

        private static class SingleTraverseResult extends LinkedHashMap<String, Object> {};
    }
}
