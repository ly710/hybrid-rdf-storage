package hybrid.rdf.storage;

import com.mongodb.client.MongoCollection;
import hybrid.rdf.storage.config.Neo4jDataSourceConfig;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;
import java.util.*;

@RestController
public class Storage {
    private final Neo4jDataSourceConfig.CypherExecutor cypherExecutor;

    private final MongoCollection<Document> mongoCollection;

    @Autowired
    public Storage(
            Neo4jDataSourceConfig.CypherExecutor cypherExecutor,
            MongoCollection<Document> mongoCollection
    ) {
        this.cypherExecutor = cypherExecutor;
        this.mongoCollection = mongoCollection;
    }

    @GetMapping("/store")
    public void store(String path) {
        Model model = getModelFromPath("file:/home/ly/下载/politicianbill.owl");

        Map<Resource, Document> rdfPropertyMaps = new HashMap<>();
        List<Document> documents = new ArrayList<>();

        StmtIterator iter = model.listStatements();

        while (iter.hasNext()) {
            Statement stmt = iter.nextStatement();// get next statement
            Resource subject = stmt.getSubject();// 获得主体
            Property predicate = stmt.getPredicate();// 获得谓语
            //此处应注意!和java中Object要区分
            RDFNode object = stmt.getObject();// 获得客体!

            System.out.println(subject.toString() + " " + predicate.toString() + " " + object.toString());

            if (object instanceof Resource) {
//                try {
//                    cypherExecutor.execute(
//                            "CREATE p = (" + subject.getLocalName() + ") -[:" + predicate.getLocalName() + "]->" + "(" + ((Resource) object).getLocalName() + ")"
//                    );
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            } else {
//                if(rdfPropertyMaps.get(subject) == null) {
//                    Document document = new Document("@id", subject.getURI());
//                    rdfPropertyMaps.put(subject, document);
//                }
//
//                Document document = new Document();
//                document.append("subject", subject.getURI());
//                document.append("predict", predicate.getURI());
//                document.append("object", object.toString());
//                documents.add(document);
            }
        }

//        System.out.println(mongoCollection.insertMany(documents));
    }

    @GetMapping("/query")
    public void query() {
        Model model = getModelFromPath("file:/home/ly/下载/politicianbill.owl");

        String queryString = "PREFIX he: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>\n" +
                "SELECT ?s \n" +
                "WHERE { \n" +
                "        ?s he:subject \"Energy\" .\n" +
                "        ?s he:number \"683\"  .    \n" +
                "}";
        Query query = QueryFactory.create(queryString);

        QueryExecution qexec = QueryExecutionFactory.create(query, model);
        ResultSet results = qexec.execSelect();
//System.out.println(results.getRowNumber());
        while(results.hasNext()) {
            QuerySolution soln = results.nextSolution();
            System.out.println(soln);
//            RDFNode x = soln.get("s");
//            System.out.println(x);
//            Resource r = soln.getResource("s");
//            System.out.println(r);
//            Literal l = soln.getLiteral("s");
//            System.out.println(l);
        }
    }

//    @GetMapping("/parse")
    public static void main(String[] args) {
        String sql = " PREFIX wd: <http://www.wikidata.org/entity/>\n" +
                "PREFIX wds: <http://www.wikidata.org/entity/statement/>\n" +
                "PREFIX wdv: <http://www.wikidata.org/value/>\n" +
                "PREFIX wdt: <http://www.wikidata.org/prop/direct/>\n" +
                "PREFIX wikibase: <http://wikiba.se/ontology#>\n" +
                "PREFIX p: <http://www.wikidata.org/prop/>\n" +
                "PREFIX ps: <http://www.wikidata.org/prop/statement/>\n" +
                "PREFIX pq: <http://www.wikidata.org/prop/qualifier/>\n" +
                "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" +
                "PREFIX bd: <http://www.bigdata.com/rdf#>\n" +
                "\n" +
                "PREFIX wdref: <http://www.wikidata.org/reference/>\n" +
                "PREFIX psv: <http://www.wikidata.org/prop/statement/value/>\n" +
                "PREFIX psn: <http://www.wikidata.org/prop/statement/value-normalized/>\n" +
                "PREFIX pqv: <http://www.wikidata.org/prop/qualifier/value/>\n" +
                "PREFIX pqn: <http://www.wikidata.org/prop/qualifier/value-normalized/>\n" +
                "PREFIX pr: <http://www.wikidata.org/prop/reference/>\n" +
                "PREFIX prv: <http://www.wikidata.org/prop/reference/value/>\n" +
                "PREFIX prn: <http://www.wikidata.org/prop/reference/value-normalized/>\n" +
                "PREFIX wdno: <http://www.wikidata.org/prop/novalue/>\n" +
                "PREFIX wdata: <http://www.wikidata.org/wiki/Special:EntityData/>\n" +
                "\n" +
                "PREFIX schema: <http://schema.org/>\n" +
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" +
                "PREFIX skos: <http://www.w3.org/2004/02/skos/core#>\n" +
                "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                "PREFIX prov: <http://www.w3.org/ns/prov#>\n" +
                "PREFIX bds: <http://www.bigdata.com/rdf/search#>\n" +
                "PREFIX gas: <http://www.bigdata.com/rdf/gas#>\n" +
                "PREFIX hint: <http://www.bigdata.com/queryHints#>" +
                "SELECT ?object (COUNT(?subject) AS ?count)\n" +
                "               (MIN(?subject) AS ?subject1) (MAX(?subject) AS ?subject2)\n" +
                "               (GROUP_CONCAT(DISTINCT ?subjectLabel; SEPARATOR=\", \") AS " +
                "?subjectLabels)\n" +
                "WHERE\n" +
                "{\n" +
                "  ?subject wdt:P238 ?object.         # IATA airport code\n" +
//                "  SERVICE wikibase:label { bd:serviceParam wikibase:language \"en\". \n" +
                "  ?subject rdfs:label ?subjectLabel.\n" +
//                "                         }\n" +
                "}\n" +
                "GROUP BY ?object\n" +
                "HAVING(COUNT(?subject) > 1)\n" +
                "ORDER BY ?object";
        Query query = QueryFactory.create(sql);
//        query.isSelectType() && query.isQueryResultStar(); // of the form SELECT *?
        query.getDatasetDescription(); // FROM / FROM NAMED bits
        query.getQueryPattern(); // The meat of the query, the WHERE bit...etc etc..
        Op op = Algebra.compile(query); // Get the algebra for th
        OpWalker.walk(op, new Visitor());
        System.out.println(op);
    }

    public static class DivideVisitor extends OpVisitorBase {
        private List<Var> neoVars = new ArrayList<>();

        private List<Var> mongoVars = new ArrayList<>();

        private List<Var> subjectVars = new ArrayList<>();

        private Op neoOp;

        private Op mongoOp;

        @Override
        public void visit(OpBGP opBGP) {
            BasicPattern neoBasicPattern = new BasicPattern();
            BasicPattern mongoBasicPattern = new BasicPattern();

            for (Triple triple : opBGP.getPattern()) {
                subjectVars.add(Var.alloc(triple.getSubject().getName()));

                if(isNeoVar(triple)) {
                    neoBasicPattern.add(triple);
                } else {
                    mongoBasicPattern.add(triple);
                }
            }

            neoOp = new OpBGP(neoBasicPattern);
            mongoOp = new OpBGP(mongoBasicPattern);
        }

        public Boolean isNeoVar(Triple triple) {
            return false;
        }

        public void visit(OpGroup opGroup) {
            for (Var var : opGroup.getGroupVars().getVars()) {
                if(subjectVars.contains(var)) {
                    if(sub)
                }
            }
        }

        @Override
        public void visit(OpGroup opGroup)
        {
            List<ExprAggregator> neo4jList = new ArrayList<>();
            List<ExprAggregator> mongoList = new ArrayList<>();

            for (ExprAggregator aggregator : opGroup.getAggregators())
            {
                if(subjectList.contains(opGroup.getGroupVars().getVars().get(0).toString())) {
                    if(neoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                    } else if(mongoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    } else {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    }
                } else if(neoVList.contains(opGroup.getGroupVars().getVars().get(0).toString())) {
                    if(neoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                    } else if(mongoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        //...
                    } else {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                    }
                } else {
                    if(neoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        //                        neo4jList.add(aggregator);
                    } else if(mongoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    } else {
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    }
                }
            }

            neo4jOp = new OpGroup(getNeoBGP(), opGroup.getGroupVars(), neo4jList);
            mongoOp = new OpGroup(getMongoBGP(), opGroup.getGroupVars(), mongoList);
        }
    }

    public static class Visitor extends OpVisitorBase
    {
        private List<String> subjectList = new ArrayList<>();
        private List<String> neoVList = new ArrayList<>();
        private List<String> mongoVList = new ArrayList<>();

        private List<Triple> neoTripleList = new ArrayList<>();
        private List<Triple> mongoTripleList = new ArrayList<>();
        private OpBGP neoBGP;
        private OpBGP mongoBGP;
        private Op neo4jOp;
        private Op mongoOp;
        private OpExtend neo4jExtend;
        private OpExtend mongoExtend;

        @Override
        public void visit(OpProject opProject)
        {
//            System.out.println("opProject");
//            System.out.println(opProject.getVars());
        }

        @Override
        public void visit(OpFilter opFilter)
        {
            for (Expr expr : opFilter.getExprs())
            {
                if(neoVList.contains(expr.getFunction().getArgs().get(0).toString())) {
                    neo4jOp = OpFilter.filter(expr, neo4jOp);
                } else if(mongoVList.contains(expr.getFunction().getArgs().get(0).toString())) {
                    mongoOp = OpFilter.filter(expr, mongoOp);
                }
            }

            System.out.println(neo4jOp);
        }

        @Override
        public void visit(OpExtend opExtend)
        {
            if(mongoVList.contains(opExtend.getVarExprList().getExprs().values().toArray()[0].toString())) {
                mongoOp = OpExtend.create(mongoOp, opExtend.getVarExprList());
            }
            else if(neoVList.contains(opExtend.getVarExprList().getExprs().values().toArray()[0].toString())) {
                neo4jOp = OpExtend.create(neo4jOp, opExtend.getVarExprList());
            }
        }

        @Override
        public void visit(OpGroup opGroup)
        {
            System.out.println(opGroup.getGroupVars().getVars());
            List<ExprAggregator> neo4jList = new ArrayList<>();
            List<ExprAggregator> mongoList = new ArrayList<>();

            for (ExprAggregator aggregator : opGroup.getAggregators())
            {
                if(subjectList.contains(opGroup.getGroupVars().getVars().get(0).toString())) {
                    if(neoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                    } else if(mongoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    } else {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    }
                } else if(neoVList.contains(opGroup.getGroupVars().getVars().get(0).toString())) {
                    if(neoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                    } else if(mongoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        //...
                    } else {
                        neo4jList.add(aggregator);
                        neoVList.add(aggregator.getVar().toString());
                    }
                } else {
                    if(neoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
//                        neo4jList.add(aggregator);
                    } else if(mongoVList.contains(aggregator.getAggregator().getExprList().get(0).toString())) {
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    } else {
                        mongoList.add(aggregator);
                        mongoVList.add(aggregator.getVar().toString());
                    }
                }
            }

            neo4jOp = new OpGroup(getNeoBGP(), opGroup.getGroupVars(), neo4jList);
            mongoOp = new OpGroup(getMongoBGP(), opGroup.getGroupVars(), mongoList);
        }

        @Override
        public void visit(OpBGP opBGP)
        {
            for (Triple triple : opBGP.getPattern()) {
                visit(triple);
            }
        }

        public void visit(Triple triple)
        {
            subjectList.add(triple.getSubject().toString());
            if(triple.getPredicate().toString().equals("http://www.wikidata.org/prop/direct/P238")) {
                if(triple.getObject().isVariable()) {
                    neoTripleList.add(triple);
                    if(triple.getObject().isVariable()) {
                        neoVList.add(triple.getObject().toString());
                    }
                }
            } else if(triple.getPredicate().toString().equals("http://www.w3.org/2000/01/rdf-schema#label")) {
                if(triple.getObject().isVariable()) {
                    mongoTripleList.add(triple);
                    mongoVList.add(triple.getObject().toString());
                }
            }
        }

        public void visit(OpJoin opJoin)
        {
//            System.out.println(opJoin.getLeft().getClass());
//            System.out.println(opJoin.getRight());
        }

        public void visit(OpService opService)
        {
//            System.out.println("opService");
//            System.out.println(opService);
//            System.out.println(opService.getService());
//            System.out.println(opService.getServiceElement());
//            System.out.println(opService.getSilent());
        }

        public void visit(OpOrder opOrder)
        {
//            System.out.println("opOrder");
        }

        public OpBGP getNeoBGP() {
            if(neoBGP == null) {
                BasicPattern basicPattern = new BasicPattern();
                for (Triple triple : neoTripleList) {
                    basicPattern.add(triple);
                }
                neoBGP = new OpBGP(basicPattern);
            }

            return neoBGP;
        }

        public OpBGP getMongoBGP() {
            if(mongoBGP == null) {
                BasicPattern basicPattern = new BasicPattern();
                for (Triple triple : mongoTripleList) {
                    basicPattern.add(triple);
                }
                mongoBGP = new OpBGP(basicPattern);
            }

            return mongoBGP;
        }
    }

    private Model getModelFromPath(String path)
    {
        InputStream in = RDFDataMgr.open(path);
        Model model = ModelFactory.createDefaultModel();
        model.read(in, null);
        return model;
    }
}
