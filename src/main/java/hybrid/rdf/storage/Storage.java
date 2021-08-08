package hybrid.rdf.storage;

import com.mongodb.client.MongoCollection;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JGraph;
import com.steelbridgelabs.oss.neo4j.structure.Neo4JVertex;
import com.steelbridgelabs.oss.neo4j.structure.providers.Neo4JNativeElementIdProvider;
import hybrid.rdf.storage.client.Neo4jClient;
import hybrid.rdf.storage.client.OntopClient;
import hybrid.rdf.storage.client.ResultList;
import hybrid.rdf.storage.config.Neo4jDataSourceConfig;
import hybrid.rdf.storage.domain.dao.*;
import hybrid.rdf.storage.domain.model.LongTripleTuple;
import hybrid.rdf.storage.domain.model.NumberPropertyInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.atlas.lib.Bytes;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Triple;
import org.apache.jena.ontology.*;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.rdf.model.impl.ResourceImpl;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.sparql.SparqlToGremlinCompiler;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.bson.Document;
import org.codelibs.minhash.MinHash;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@RestController
public class Storage {
    private final Neo4jDataSourceConfig.CypherExecutor cypherExecutor;

    private final MongoCollection<Document> mongoCollection;

    private final Neo4jClient neo4jClient;

    private final OntopClient ontopClient;

    private final SpoDaoIRepo spoDaoIRepo;

    private final SpoDaoSRepo spoDaoSRepo;

    private final SubjectRepo subjectRepo;

    private final ObjectRepo objectRepo;

    private final PredictRepo predictRepo;

    @Autowired
    public Storage(
            Neo4jDataSourceConfig.CypherExecutor cypherExecutor,
            MongoCollection<Document> mongoCollection,
            Neo4jClient neo4jClient,
            OntopClient ontopClient,
            SpoDaoIRepo spoDaoIRepo,
            SpoDaoSRepo spoDaoSRepo,
            SubjectRepo subjectRepo,
            ObjectRepo objectRepo,
            PredictRepo predictRepo
    ) {
        this.cypherExecutor = cypherExecutor;
        this.mongoCollection = mongoCollection;
        this.neo4jClient = neo4jClient;
        this.ontopClient = ontopClient;
        this.spoDaoIRepo = spoDaoIRepo;
        this.spoDaoSRepo = spoDaoSRepo;
        this.subjectRepo = subjectRepo;
        this.objectRepo = objectRepo;
        this.predictRepo = predictRepo;
    }

    public static String escapeQueryChars(String s) {
        if (StringUtils.isBlank(s)) {
            return s;
        }
        StringBuilder sb = new StringBuilder();
        //查询字符串一般不会太长，挨个遍历也花费不了多少时间
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            // These characters are part of the query syntax and must be escaped
            if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')'
                    || c == ':' || c == '^'	|| c == '[' || c == ']' || c == '\"'
                    || c == '{' || c == '}' || c == '~' || c == '*' || c == '?'
                    || c == '|' || c == '&' || c == ';' || c == '/' || c == '.'
                    || c == '$' || Character.isWhitespace(c)) {
                sb.append('\\');
            }
            sb.append(c);
        }
        return sb.toString();
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

//            System.out.println(subject.toString() + " " + predicate.toString() + " " + object.toString());

            if (object instanceof Resource) {
                try
                {
                    subjectRepo.save(subject.toString());
                    objectRepo.save(object.toString());
//                    predictRepo.save(predicate.toString());
                } catch (Exception e) {
                    //..
                }

//                System.out.println(subject + " " + predicate + " " + object);
//                try {
//                    String objectUri = ((Resource) object).getURI();
//                    cypherExecutor.execute(
//                            "CREATE p = ({name: '" + subject.getURI() + "'}) -[:`" + predicate.getURI() + "`]->" + "({name:'" + objectUri + "'})"
//                    );
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            } else {
                if(isInteger(object.toString())) {
//                    spoDaoIRepo.save(subject.getURI(), predicate.getURI(), Integer.valueOf(object.toString()));
                } else {
//                    spoDaoSRepo.save(subject.getURI(), predicate.getURI(), object.toString());
                }
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

    @GetMapping("/query-neo4j")
    public void queryNeo4j() {
        Driver driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "1"));
        Neo4JElementIdProvider<Long> provider = new Neo4JNativeElementIdProvider();

        try
        {
            Graph graph = new Neo4JGraph(driver, "neo4j", provider, provider);
            Transaction transaction = graph.tx();

            String sparql = "PREFIX house: <http://www.house.gov/members#>\n" +
                    "PREFIX pb: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>" +
                    "select (count(?s) as ?ss) where { ?s v:name \"http://www.house.gov/members#P000422\" . ?s e:http:\\/\\/swat\\.cse\\.lehigh\\.edu\\/resources\\/onto\\/politicianbill.owl\\#sponsoredBill ?o .} group by ?s";

            System.out.println(sparql);
//                    "select ?s ?o where { ?s v:name \"http://www.house.gov/members#P000422\" . ?s e ?o .}";
//                    "select ?s where { ?s p:name e:P000422 . ?s e:house:}";
            GraphTraversal<Vertex, ?> graphTraversal = SparqlToGremlinCompiler.compile(graph, sparql);

            while (graphTraversal.hasNext())
            {
                System.out.println(graphTraversal.next());
                HashMap<String, Neo4JVertex> vertex = (HashMap<String, Neo4JVertex>) graphTraversal.next();
                System.out.println(vertex.get("s").property("name").value());
//                System.out.println(vertex.get("o").property("name").value());
//                System.out.println(graphTraversal.next().getClass());
//                graphTraversal.next();
            }

            graphTraversal.close();
            transaction.commit();
            graph.close();
            driver.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/query-neo4j-gremlin")
    public void sparqlGremlinTest() {
        String sparql = "PREFIX house: <http://www.house.gov/members#>\n" +
                "PREFIX pb: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>" +
                "select ?o where { house:P000422 pb:sponsoredBill ?o}";

        Query query = QueryFactory.create(sparql);
        Op op = Algebra.compile(query);

        System.out.println(neo4jClient.find(op));

//        System.out.println((OpProject)divideVisitor.getNeoOp());
//        System.out.println(op);
    }

    @GetMapping("/query-ontop")
    public void queryOnTop() {
        String sparql = "PREFIX voc: <http://example.org/voc#>\n" +
                "SELECT * WHERE { voc:s2 voc:dfsdfp ?o } ";

        Query query = QueryFactory.create(sparql);
        Op op = Algebra.compile(query);

        System.out.println(ontopClient.find(op));
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
//        DivideVisitor divideVisitor = new DivideVisitor();
//        OpWalker.walk(op, divideVisitor);
//        System.out.println(divideVisitor.getNeoOp());
//        System.out.println(divideVisitor.getMongoOp());
//        OpExecutor

//        System.out.println((OpProject)divideVisitor.getNeoOp());
//        System.out.println(op);
    }

    @GetMapping("/hybrid-query")
    public void hybridQuery() {
        OntModel ontModel = ModelFactory.createOntologyModel();
        ontModel.read("/home/ly/下载/politicianbill.owl");

        String queryString = "PREFIX he: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>\n" +
                "SELECT ?s ?o \n" +
                "WHERE { \n" +
                "        ?s <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#sponsoredBill> ?o .\n" +
                "        ?o he:number 683  .    \n" +
                "}";
        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);
        DivideVisitor divideVisitor = new DivideVisitor(ontModel);
        OpWalker.walk(op, divideVisitor);

        System.out.println(divideVisitor.getNeoOp());
        System.out.println(divideVisitor.getMongoOp());

        ResultList neoList = neo4jClient.find(divideVisitor.getNeoOp());
        ResultList mongoList = ontopClient.find(divideVisitor.getMongoOp());

        System.out.println(neoList);
        System.out.println(mongoList);

        ResultList resultList = new ResultList();

        for (Map<String, ?> stringMap : neoList) {
            for (Map<String, ?> map : mongoList) {
                if(stringMap.get("o").equals(map.get("o"))) {
                    resultList.add(stringMap);
                }
            }
        }

        System.out.println(resultList);
    }

    @GetMapping("/query")
    public void query() {
        Model model = getModelFromPath("file:/home/ly/下载/politicianbill.owl");

        String queryString = "PREFIX he: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>\n" +
                "SELECT ?s ?o \n" +
                "WHERE { \n" +
                "        ?s he:sponsoredBill ?o .\n" +
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

    @GetMapping("/ontology")
    public void processOntoloy() {
        OntModel m = ModelFactory.createOntologyModel();
        m.read("http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl");

        DatatypeProperty numberTypeProperty = m.getDatatypeProperty("http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#number");
        numberTypeProperty.addRange(new ResourceImpl(XSDDatatype.XSDinteger.getURI()));

        DatatypeProperty congressTypeProperty = m.getDatatypeProperty("http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#congress");
        congressTypeProperty.addRange(new ResourceImpl(XSDDatatype.XSDinteger.getURI()));

        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/home/ly/下载/politicianbill.owl"));
            m.write(out);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @GetMapping("/minHashTest")
    public void minHash() throws IOException {
        Tokenizer tokenizer = new Tokenizer()
        {
            @Override
            public boolean incrementToken() throws IOException
            {
                return false;
            }
        };

        // The number of bits for each hash value.
        int hashBit = 1;
        // A base seed for hash functions.
        int seed = 0;
        // The number of hash functions.
        int num = 128;
        // Analyzer for 1-bit 128 hash with default Tokenizer (WhitespaceTokenizer).
        Analyzer analyzer = MinHash.createAnalyzer(new WhitespaceTokenizer(), hashBit, seed, num);

        String text = "a";

        // Calculate a minhash value. The size is hashBit*num.
        byte[] minhash = MinHash.calculate(analyzer, text);


//        String text1 = "Fess is very powerful and easily deployable Search Server.";
        String text1 = "k";
        byte[] minhash1 = MinHash.calculate(analyzer, text1);
        System.out.println(MinHash.compare(minhash, minhash1));
    }

    @GetMapping("/minHash1")
    public void minHash1() {
        // Initialize the hash function for an similarity error of 0.1
        // For sets built from a dictionary of 5 items
        info.debatty.java.lsh.MinHash minhash = new info.debatty.java.lsh.MinHash(0.05, 20);

        // Sets can be defined as an vector of booleans:
        // [1 0 0 1 0]
        Set<Integer> set1 = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
        int[] sig1 = minhash.signature(set1);

        // Or as a set of integers:
        // set2 = [1 0 1 1 0]
//        TreeSet<Integer> set2 = new TreeSet<Integer>();
//        set2.add(0);
//        set2.add(2);
//        set2.add(3);
        Set<Integer> set2 = new HashSet<>(Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 2, 1));
        int[] sig2 = minhash.signature(set2);

        System.out.println("Signature similarity: " + minhash.similarity(sig1, sig2));
        System.out.println("Real similarity (Jaccard index)" +
                                   info.debatty.java.lsh.MinHash.jaccardIndex(set1, set2));
    }

    @GetMapping("/test-minhash")
    public void testMinHash() {
        OntModel ontModel = ModelFactory.createOntologyModel();
        ontModel.read("/home/ly/下载/politicianbill.owl");
        String p = "http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#sponsoredBy";

        String queryString = "PREFIX he: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>\n" +
                "PREFIX data: <http://swat.cse.lehigh.edu/resources/data/politicianbill.owl#>\n" +
                "PREFIX rdfs: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "SELECT ?s \n" +
                "WHERE { \n" +
                "        ?s <" + p + "> ?o .\n" +
                "}";
        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);
        DivideVisitor divideVisitor = new DivideVisitor(ontModel);
        OpWalker.walk(op, divideVisitor);

        ResultList neoList = neo4jClient.find(divideVisitor.getNeoOp());

        System.out.println(neoList);

//        Set<Integer> set1 = new HashSet<>();
//        for (Map<String, ?> stringMap : neoList) {
//            set1.add(subjectRepo.findIdByName(stringMap.get("s").toString()));
//        }
//
//
//        queryString = "PREFIX he: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>\n" +
//                "PREFIX rdfs: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
//                "SELECT ?s \n" +
//                "WHERE { \n" +
//                "        ?s rdfs:type he:Senator .\n" +
//                "}";
//
//        Query query2 = QueryFactory.create(queryString);
//        Op op2 = Algebra.compile(query2);
//        DivideVisitor divideVisitor2 = new DivideVisitor(ontModel);
//        OpWalker.walk(op2, divideVisitor2);
//
//        ResultList neoList2 = neo4jClient.find(divideVisitor2.getNeoOp());
//
//        System.out.println(neoList2);
//
//        Set<Integer> set2 = new HashSet<>();
//        for (Map<String, ?> stringMap : neoList2) {
//            set2.add(subjectRepo.findIdByName(stringMap.get("s").toString()));
//        }
//
//        System.out.println(set1);
//        System.out.println(set2);
//
//        info.debatty.java.lsh.MinHash minhash = new info.debatty.java.lsh.MinHash(0.05, 10000);
//
//        // Sets can be defined as an vector of booleans:
//        // [1 0 0 1 0]
//        int[] sig1 = minhash.signature(set1);
//
//        System.out.println(sig1.length);
//        // Or as a set of integers:
//        // set2 = [1 0 1 1 0]
//        //        TreeSet<Integer> set2 = new TreeSet<Integer>();
//        //        set2.add(0);
//        //        set2.add(2);
//        //        set2.add(3);
//        int[] sig2 = minhash.signature(set2);
//        System.out.println(sig2.length);
//        System.out.println("Signature similarity: " + minhash.similarity(sig1, sig2));
//        System.out.println("Real similarity (Jaccard index)" +
//                                   info.debatty.java.lsh.MinHash.jaccardIndex(set1, set2));
    }

    @GetMapping("/test-number")
    public void testNumberProperty() {
        NumberPropertyInfo numberPropertyInfo = spoDaoIRepo.getNumberPropertyInfo("http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#number");
        System.out.println(numberPropertyInfo);

        List<LongTripleTuple> tripleTuples = spoDaoIRepo.getNumberPropertySampleList(numberPropertyInfo.getTotalCount() / 100, "http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#number");

        Set<Integer> ids = spoDaoIRepo.getSidByORange(tripleTuples.get(0).getRownum(), tripleTuples.get(1).getRownum(), "http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#number");
        info.debatty.java.lsh.MinHash minhash = new info.debatty.java.lsh.MinHash(0.1, ids.size());

        System.out.println(minhash.signature(ids).length);
    }

    @GetMapping("/generate-minHash")
    public void generateMinHash() {
        OntModel ontModel = ModelFactory.createOntologyModel();
        ontModel.read("/home/ly/下载/politicianbill.owl");
        String s = "http://swat.cse.lehigh.edu/resources/data/politicianbill.owl#h1128";
        String p = "http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#sponsoredBy";

        String queryString = "PREFIX he: <http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#>\n" +
                "PREFIX data: <http://swat.cse.lehigh.edu/resources/data/politicianbill.owl#>\n" +
                "PREFIX rdfs: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                "SELECT ?s \n" +
                "WHERE { \n" +
                "        <" + s + "> <" + p + "> ?o .\n" +
                "}";
        Query query = QueryFactory.create(queryString);
        Op op = Algebra.compile(query);
        DivideVisitor divideVisitor = new DivideVisitor(ontModel);
        OpWalker.walk(op, divideVisitor);

        ResultList neoList = neo4jClient.find(divideVisitor.getNeoOp());

        System.out.println(neoList);

        Set<Integer> ids = new HashSet<>();
        for (Map<String, ?> stringMap : neoList) {
            ids.add(objectRepo.findIdByName(stringMap.get("o").toString()));
        }

        info.debatty.java.lsh.MinHash minhash = new info.debatty.java.lsh.MinHash(0.1, ids.size());

        // Sets can be defined as an vector of booleans:
        // [1 0 0 1 0]
        int[] sig1 = minhash.signature(ids);

        Jedis jedis = new Jedis("localhost", 6379);
        String key = s + ":" + p;

        System.out.println(sig1.length);

//        for (int i : sig1)
//        {
//            Bytes.intToBytes(i);
//            Bytes.getInt(Bytes.intToBytes(i));
//            jedis.rpush(Bytes.string2bytes(key), Bytes.intToBytes(i));
//        }

        System.out.println(Bytes.getInt(jedis.lpop(Bytes.string2bytes(key))));
    }

    public static class DivideVisitor extends OpVisitorBase {
        private enum VariableAffiliation {
            MONGO, NEO, BOTH;
        }

        private final ExtendVarSet neoVars = new ExtendVarSet();

        private final ExtendVarSet mongoVars = new ExtendVarSet();

        private Op neoOp;

        private Op mongoOp;

        private OntModel ontModel;

        public DivideVisitor(OntModel ontModel) {
            this.ontModel = ontModel;
        }

        @Override
        public void visit(OpBGP opBGP) {
            BasicPattern neoBasicPattern = new BasicPattern();
            BasicPattern mongoBasicPattern = new BasicPattern();

            for (Triple triple : opBGP.getPattern()) {
                if(isNeoTriple(triple)) {
                    neoBasicPattern.add(triple);
                    visit(triple, true);
                }

                if(isMongoTriple(triple)) {
                    mongoBasicPattern.add(triple);
                    visit(triple, false);
                }
            }

            neoOp = new OpBGP(neoBasicPattern);
            mongoOp = new OpBGP(mongoBasicPattern);
        }

        public void visit(Triple triple, Boolean isNeo) {
            if(triple.getSubject().isVariable()) {
                if(isNeo) {
                    neoVars.add(new ExtendVar(
                            Var.alloc(triple.getSubject()),
                            0
                    ));
                } else {
                    mongoVars.add(new ExtendVar(
                            Var.alloc(triple.getSubject()),
                            0
                    ));
                }
            }

            if(triple.getPredicate().isVariable()) {
                if(isNeo) {
                    neoVars.add(new ExtendVar(
                            Var.alloc(triple.getPredicate()),
                            0
                    ));
                } else {
                    mongoVars.add(new ExtendVar(
                            Var.alloc(triple.getPredicate()),
                            0
                    ));
                }
            }

            if(triple.getObject().isVariable()) {
                if(isNeo) {
                    neoVars.add(new ExtendVar(
                            Var.alloc(triple.getObject()),
                            0
                    ));
                } else {
                    mongoVars.add(new ExtendVar(
                            Var.alloc(triple.getObject()),
                            0
                    ));
                }
            }
        }

        public Boolean isNeoTriple(Triple triple) {
            return Objects.equals(ontModel.getDatatypeProperty(triple.getPredicate().getURI()), null);
//            return triple.getPredicate().toString().equals("http://swat.cse.lehigh.edu/resources/onto/politicianbill.owl#sponsoredBill");
        }

        public Boolean isMongoTriple(Triple triple) {
            return !Objects.equals(ontModel.getDatatypeProperty(triple.getPredicate().getURI()), null);
        }

        @Override
        public void visit(OpGroup opGroup) {
            List<ExprAggregator> neoAggregators = new ArrayList<>();
            List<ExprAggregator> mongoAggregators = new ArrayList<>();
            List<ExtendVar> neoGroupExprVars = new ArrayList<>();
            List<ExtendVar> mongoGroupExprVars = new ArrayList<>();

            for (ExprAggregator aggregator : opGroup.getAggregators()) {
                if(isAggVarFromNeo(aggregator)) {
                    neoAggregators.add(aggregator);
                    neoGroupExprVars.add(new ExtendVar(aggregator.getVar(), 1));
                }

                if(isAggVarFromMongo(aggregator)) {
                    mongoAggregators.add(aggregator);
                    mongoGroupExprVars.add(new ExtendVar(aggregator.getVar(), 1));
                }
            }

            List<Var> neoGroupVars = new ArrayList<>();
            List<Var> mongoGroupVars = new ArrayList<>();
            Var lastVar = opGroup.getGroupVars().getVars().get(0);
            for (Var var : opGroup.getGroupVars().getVars())
            {
                if(isVarFromNeo(var)) {
                    neoGroupVars.add(var);
                }

                if(isVarFromMongo(var)) {
                    mongoGroupVars.add(var);
                }

                if(isVarCompatibleConsiderOrder(lastVar, var)) {
                    lastVar = var;
                } else {
                    break;
                }
            }


            List<Var> nonJoinVars = new ArrayList<>(neoVars.vars);
            nonJoinVars.removeAll(new ArrayList<>(mongoVars.vars));
            List<Var> joinVars = new ArrayList<>(neoVars.vars);
            joinVars.removeAll(nonJoinVars);

            if(!neoGroupVars.isEmpty() && joinVars.containsAll(neoGroupVars)) {
                neoOp = new OpGroup(neoOp, new VarExprList(neoGroupVars), neoAggregators);
                neoVars.addAll(neoGroupExprVars);
            }

            if(!mongoGroupVars.isEmpty() && joinVars.containsAll(mongoGroupVars)) {
                mongoOp = new OpGroup(mongoOp, new VarExprList(mongoGroupVars), mongoAggregators);
                mongoVars.addAll(mongoGroupExprVars);
            }
        }

        public Boolean isAggVarFromNeo(ExprAggregator exprAggregator) {
            return neoVars.contains(exprAggregator.getAggregator().getExprList().get(0).asVar());
        }

        public Boolean isAggVarFromMongo(ExprAggregator exprAggregator) {
            return mongoVars.contains(exprAggregator.getAggregator().getExprList().get(0).asVar());
        }

        public Boolean isVarFromNeo(Var var) {
            return neoVars.contains(var);
        }

        public Boolean isVarFromMongo(Var var) {
            return mongoVars.contains(var);
        }

        @Override
        public void visit(OpExtend opExtend) {
            Iterator<Expr> iterator = opExtend.getVarExprList().getExprs().values().iterator();

            List<Expr> exprList = new ArrayList<>();
            while (iterator.hasNext()) {
                exprList.add(iterator.next());
            }

            if(VariableAffiliation.BOTH.equals(getArgsVarAffiliation(exprList))) {
                neoOp = OpExtend.create(neoOp, opExtend.getVarExprList());
                neoVars.add(new ExtendVar(opExtend.getVarExprList().getVars().get(0), 2));

                mongoOp = OpExtend.create(mongoOp, opExtend.getVarExprList());
                mongoVars.add(new ExtendVar(opExtend.getVarExprList().getVars().get(0), 2));
            } else if(VariableAffiliation.NEO.equals(getArgsVarAffiliation(exprList))) {
                neoOp = OpExtend.create(neoOp, opExtend.getVarExprList());
                neoVars.add(new ExtendVar(opExtend.getVarExprList().getVars().get(0), 2));
            } else if(VariableAffiliation.MONGO.equals(getArgsVarAffiliation(exprList))) {
                mongoOp = OpExtend.create(mongoOp, opExtend.getVarExprList());
                mongoVars.add(new ExtendVar(opExtend.getVarExprList().getVars().get(0), 2));
            }
        }

        @Override
        public void visit(OpFilter opFilter) {
            for (Expr expr : opFilter.getExprs())
            {
                if(VariableAffiliation.BOTH.equals(getArgsVarAffiliation(expr.getFunction().getArgs()))) {
                    neoOp = OpFilter.filter(expr, neoOp);
                    mongoOp = OpFilter.filter(expr, mongoOp);
                    continue;
                }

                if(VariableAffiliation.NEO.equals(getArgsVarAffiliation(expr.getFunction().getArgs()))) {
                    neoOp = OpFilter.filter(expr, neoOp);
                }

                if(VariableAffiliation.MONGO.equals(getArgsVarAffiliation(expr.getFunction().getArgs()))) {
                    mongoOp = OpFilter.filter(expr, mongoOp);
                }
            }
        }

        public void visit(OpOrder opOrder) {}

        public void visit(OpProject opProject) {
            neoOp = new OpProject(neoOp, neoVars.getLastLevelVars());
            mongoOp = new OpProject(mongoOp, mongoVars.getLastLevelVars());

//            System.out.println(neoOp);
//            System.out.println(mongoOp);
        }

        public VariableAffiliation getAffiliation(Var var) {
            if(neoVars.contains(var) && mongoVars.contains(var)) {
                return VariableAffiliation.BOTH;
            }

            if(neoVars.contains(var)) {
                return VariableAffiliation.NEO;
            }

            if(mongoVars.contains(var)) {
                return VariableAffiliation.MONGO;
            }

            return null;
        }

        public Boolean isVarCompatibleConsiderOrder(Var var1, Var var2) {
            if(
                    (getAffiliation(var1) == null || getAffiliation(var2) == null)
                            || (var1 == null || var2 == null)
                            || (!var1.isVariable() || !var2.isVariable())
            ) {
                return false;
            }

            if(getAffiliation(var1).equals(VariableAffiliation.BOTH)) {
                return true;
            }

            if(getAffiliation(var1).equals(getAffiliation(var2))) {
                return true;
            }

            return false;
        }

        public VariableAffiliation getArgsVarAffiliation(List<Expr> vars) {
            Expr lastVar = vars.get(0);
            VariableAffiliation affiliation = getAffiliation(lastVar.asVar());

            for (Expr var : vars) {
                if(isVarCompatibleConsiderOrder(lastVar.asVar(), var.asVar())) {
                    lastVar = var;
                    if(affiliation.equals(VariableAffiliation.BOTH)) {
                        affiliation = getAffiliation(var.asVar());
                    }
                } else {
                    affiliation = null;
                    break;
                }
            }

            return affiliation;
        }

        public Op getNeoOp() {
            return neoOp;
        }

        public void setNeoOp(Op neoOp) {
            this.neoOp = neoOp;
        }

        public Op getMongoOp() {
            return mongoOp;
        }

        public void setMongoOp(Op mongoOp) {
            this.mongoOp = mongoOp;
        }
    }

    public static class ExtendVar {
        private final Var var;

        private final Integer level;

        private final List<ExtendVar> subVars;

        public ExtendVar(Var var, int level, ExtendVar...extendVars) {
            this.var = var;
            this.level = level;
            subVars = Arrays.asList(extendVars);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ExtendVar extendVar = (ExtendVar) o;
            return Objects.equals(var, extendVar.var) &&
                    Objects.equals(level, extendVar.level) &&
                    Objects.equals(subVars, extendVar.subVars);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(var, level, subVars);
        }

        @Override
        public String toString()
        {
            return "ExtendVar{" +
                    "var=" + var +
                    ", level=" + level +
                    '}';
        }
    }

    public static class ExtendVarSet {
        private final Set<ExtendVar> extendVars = new HashSet<>();

        private final Set<Var> vars = new HashSet<>();

        private Integer maxLevel = 0;

        public ExtendVarSet() {}

        public void add(ExtendVar extendVar) {
            extendVars.add(extendVar);
            vars.add(extendVar.var);
            if(extendVar.level > maxLevel) {
                maxLevel = extendVar.level;
            }
        }

        public boolean contains(ExtendVar extendVar) {
            return vars.contains(extendVar.var);
        }

        public boolean contains(Var var) {
            return vars.contains(var);
        }

        public List<ExtendVar> getLastLevelExtendVars() {
            List<ExtendVar> extendVarList = new ArrayList<>();

            for (ExtendVar extendVar : extendVars) {
                if(extendVar.level.equals(maxLevel)) {
                    extendVarList.add(extendVar);
                }
            }

            return extendVarList;
        }

        public List<Var> getLastLevelVars() {
            List<Var> varList = new ArrayList<>();

            for (ExtendVar extendVar : extendVars) {
                if(extendVar.level.equals(maxLevel)) {
                    varList.add(extendVar.var);
                }
            }

            return varList;
        }

        public void addAll(List<ExtendVar> vs) {
            for (ExtendVar var : vs) {
                add(var);
            }
        }

        @Override
        public String toString()
        {
            return "ExtendVarSet{" +
                    "extendVars=" + extendVars +
                    '}';
        }
    }

    private Model getModelFromPath(String path)
    {
        InputStream in = RDFDataMgr.open(path);
        Model model = ModelFactory.createDefaultModel();
        model.read(in, null);
        return model;
    }

    public static boolean isInteger(String s) {
        return isInteger(s,10);
    }

    public static boolean isInteger(String s, int radix) {
        if(s.isEmpty()) return false;
        for(int i = 0; i < s.length(); i++) {
            if(i == 0 && s.charAt(i) == '-') {
                if(s.length() == 1) return false;
                else continue;
            }
            if(Character.digit(s.charAt(i),radix) < 0) return false;
        }
        return true;
    }
}

