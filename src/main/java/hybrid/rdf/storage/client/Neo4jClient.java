package hybrid.rdf.storage.client;

import org.apache.jena.graph.*;
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

    public ResultList find(Op op) {
        Op sparGremlinOp = Transformer.transform(new OpTransfer(), op);
        Query q = OpAsQuery.asQuery(sparGremlinOp);
        q.setQuerySelectType();
        String sparql = q.serialize()
                .replace("<", "")
                .replace(">", "")
                .replace("\\\\", "\\");

        GraphTraversal<Vertex, ?> graphTraversal = SparqlToGremlinCompiler.compile(graph, sparql);

        SparqlGremlinGraphTraverser sparqlGremlinGraphTraverser = new SparqlGremlinGraphTraverser(
                graphTraversal,
                q.getResultVars()
        );

        sparqlGremlinGraphTraverser.traverse();

        ResultList resultMappings = sparqlGremlinGraphTraverser.getMappingList();

        try {
            graphTraversal.close();
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return resultMappings;
    }

    private static class SparqlGremlinGraphTraverser {
        private final GraphTraversal<Vertex, ?> graphTraversal;

        private final ResultList mappingList = new ResultList();

        private List<String> vars;

        public SparqlGremlinGraphTraverser(
                GraphTraversal<Vertex, ?> graphTraversal,
                List<String> vars
        ) {
            this.graphTraversal = graphTraversal;
            this.vars = vars;
        }

        public void traverse() {
            while (graphTraversal.hasNext()) {
                ResultList.Mappings mappings = new ResultList.Mappings(vars, graphTraversal.next());
                mappingList.add(mappings.getMappings());
            }
        }

        public ResultList getMappingList() {
            return mappingList;
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
            return NodeFactory.createURI(escapeEdgeLiteral("e:" + edge));
        }

        private String escapeEdgeLiteral(String s) {
            return s.replace("/", "\\/").replace("#", "\\#");
        }

        private String quoteEntity(String s) {
            return "\"" + s + "\"";
        }
    }
}
