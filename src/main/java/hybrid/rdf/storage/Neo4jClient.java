package hybrid.rdf.storage;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JVertex;
import groovy.json.StringEscapeUtils;
import lombok.Data;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.*;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.query.Query;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.sparql.SparqlToGremlinCompiler;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class Neo4jClient {
    private final Graph graph;

    public Neo4jClient(Graph graph) {
        this.graph = graph;
    }

    public ResultMappings find(Op op) {
        Op sparGremlinOp = Transformer.transform(new OpTransfer(), op);
        Query q = OpAsQuery.asQuery(sparGremlinOp);
        q.setQuerySelectType();
        String sparql = q.serialize();

        System.out.println(sparql);

        GraphTraversal<Vertex, ?> graphTraversal = SparqlToGremlinCompiler.compile(graph, sparql);

        SparqlGremlinGraphTraverser sparqlGremlinGraphTraverser = new SparqlGremlinGraphTraverser(
                graphTraversal,
                q.getResultVars()
        );

        sparqlGremlinGraphTraverser.traverse();

        ResultMappings resultMappings = sparqlGremlinGraphTraverser.getMappings();

        try {
            graphTraversal.close();
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultMappings;
    }

    private static class ResultMappings extends ArrayList<Map<String, String>> {}

    private static class SparqlGremlinGraphTraverser {
        private final GraphTraversal<Vertex, ?> graphTraversal;

        private final ResultMappings mappings = new ResultMappings();

        private List<String> vars = new ArrayList<>();

        public SparqlGremlinGraphTraverser(
                GraphTraversal<Vertex, ?> graphTraversal,
                List<String> vars
        ) {
            this.graphTraversal = graphTraversal;
            this.vars = vars;
        }

        public void traverse() {
            while (graphTraversal.hasNext()) {
                HashMap<String, Neo4JVertex> vertex = (HashMap<String, Neo4JVertex>) graphTraversal.next();
                Map<String, String> mapping = new HashMap<>();
                for (String var : vars) {
                    mapping.put(var, vertex.get(var).property("name").value().toString());
                }

                mappings.add(mapping);
            }
        }

        public ResultMappings getMappings() {
            return mappings;
        }
    }

    private static class OpTransfer extends TransformBase {
        @Override
        public Op transform(OpBGP opBGP) {
            BasicPattern originPattern = opBGP.getPattern();
            BasicPattern sparqlGremlinQueryPattern = new BasicPattern();

            for (int i = 0; i < originPattern.size(); i++) {
                Triple triple = originPattern.get(i);

                if (!triple.getSubject().isVariable()) {
                    Node sparqlGremlinSubjectVar = new Node_Variable(getSparqlGremlinSubjectVar(i));

                    Triple nameTriple = new Triple(
                            sparqlGremlinSubjectVar,
                            getSparqlGremlinNameEdge(),
                            NodeFactory.createLiteral(triple.getSubject().toString())
                    );

                    sparqlGremlinQueryPattern.add(nameTriple);

                    if (!triple.getObject().isVariable()) {
                        Node sparqlGremlinObjectVar = new Node_Variable(getSparqlGremlinObjectVar(i));

                        Triple queryTriple = new Triple(
                                sparqlGremlinSubjectVar,
                                getSparqlGremlinEdgeNode(triple.getPredicate().toString()),
                                sparqlGremlinObjectVar
                        );

                        Triple objectNameTriple = new Triple(
                                sparqlGremlinSubjectVar,
                                getSparqlGremlinNameEdge(),
                                NodeFactory.createLiteral(triple.getObject().toString())
                        );

                        sparqlGremlinQueryPattern.add(queryTriple);
                        sparqlGremlinQueryPattern.add(objectNameTriple);
                    } else {
                        Triple queryTriple = new Triple(
                                sparqlGremlinSubjectVar,
                                getSparqlGremlinEdgeNode(triple.getPredicate().toString()),
                                triple.getObject()
                        );

                        sparqlGremlinQueryPattern.add(queryTriple);
                    }
                } else {
                    if (!triple.getObject().isVariable())
                    {
                        Node sparqlGremlinObjectVar = new Node_Variable(
                                getSparqlGremlinObjectVar(i));

                        Triple queryTriple = new Triple(
                                triple.getSubject(),
                                getSparqlGremlinEdgeNode(triple.getPredicate().toString()),
                                sparqlGremlinObjectVar
                        );

                        Triple nameTriple = new Triple(
                                sparqlGremlinObjectVar,
                                getSparqlGremlinNameEdge(),
                                NodeFactory.createLiteral(triple.getObject().toString())
                        );

                        sparqlGremlinQueryPattern.add(queryTriple);
                        sparqlGremlinQueryPattern.add(nameTriple);
                    } else {
                        Triple queryTriple = new Triple(
                                triple.getSubject(),
                                getSparqlGremlinEdgeNode(triple.getPredicate().toString()),
                                triple.getObject()
                        );

                        sparqlGremlinQueryPattern.add(queryTriple);
                    }
                }
            }

            opBGP.getPattern().getList().removeAll(opBGP.getPattern().getList());
            opBGP.getPattern().addAll(sparqlGremlinQueryPattern);

            return opBGP;
//
//            return new OpBGP(sparqlGremlinQueryPattern);
        }

        private String getSparqlGremlinSubjectVar(int n) {
            return "sparqlGremlinSubjectVar" + n;
        }

        private Node getSparqlGremlinNameEdge() {
            return new Node_Concrete("v:name")
            {
                @Override
                public Object visitWith(NodeVisitor v)
                {
                    return "v:name";
                }

                @Override
                public boolean equals(Object o)
                {
                    return false;
                }
            };
        }

        private String getSparqlGremlinObjectVar(int n) {
            return "sparqlGremlinObjectVar" + n;
        }

        private Node getSparqlGremlinEdgeNode(String edge) {
            String sparql = "PREFIX house: <http://www.house.gov/members#>\n" +
                    "PREFIX pb: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>" +
                    "select (count(?s) as ?ss) where { ?s v:name \"http://www.house.gov/members#P000422\" . ?s e:http:\\/\\/swat\\.cse\\.lehigh\\.edu\\/resources\\/onto\\/politicianbill.owl\\#sponsoredBill ?o .} group by ?s";

            String a = "a/aa";
            System.out.println(a);
            System.out.println(StringEscapeUtils.escapeJavaScript(StringEscapeUtils.escapeJavaScript(a)));
            System.out.println(StringEscapeUtils.escapeJava(a));
            System.out.println(sparql);
            System.out.println("e:" + edge);
            System.out.println("e:" + escapeEdgeLiteral(escapeEdgeLiteral(edge)));
            return NodeFactory.createLiteral(escapeEdgeLiteral("e:" + edge));
        }

        private String escapeEdgeLiteral(String s) {
            return StringEscapeUtils.escapeJavaScript(s).replace("#", "\\#");
        }

        private String quoteEntity(String s) {
            return "\"" + s + "\"";
        }
    }

    @Data
    private static class QueryPatternParser {
        private List<String> vars = new ArrayList<>();

        private BasicPattern sparqlGremlinQueryPattern = new BasicPattern();

        public QueryPatternParser(BasicPattern originPattern) {
        }

        private String getSparqlGremlinSparql() {
            String sparql = "select " + String.join(",", vars) + " where \n";
            List<String> patternStringList = new ArrayList<>();

            for (Triple triple : sparqlGremlinQueryPattern) {
                patternStringList.add(triple.getSubject().toString() + " " + triple.getPredicate().toString() + " " + triple.getObject().toString());
            }

            sparql += String.join(".", patternStringList);

            return sparql;
        }
    }

    private static class Literal implements LiteralLabel {
        private String s;

        public Literal(String s) {
            this.s = s;
        }

        @Override
        public boolean isXML() {
            return false;
        }

        @Override
        public boolean isWellFormed() {
            return false;
        }

        @Override
        public boolean isWellFormedRaw() {
            return false;
        }

        @Override
        public String toString(boolean b) {
            return null;
        }

        @Override
        public String getLexicalForm() {
            return null;
        }

        @Override
        public Object getIndexingValue() {
            return null;
        }

        @Override
        public String language() {
            return null;
        }

        @Override
        public Object getValue() throws DatatypeFormatException {
            return s;
        }

        @Override
        public RDFDatatype getDatatype() {
            return null;
        }

        @Override
        public String getDatatypeURI() {
            return null;
        }

        @Override
        public boolean sameValueAs(LiteralLabel literalLabel) {
            return false;
        }

        @Override
        public int getDefaultHashcode() {
            return 0;
        }
    }
}
