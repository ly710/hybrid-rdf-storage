package hybrid.rdf.storage;

import com.mongodb.client.MongoCollection;
import hybrid.rdf.storage.config.Neo4jDataSourceConfig;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitorBase;
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

            if (object instanceof Resource) {
//                try {
//                    cypherExecutor.execute(
//                            "CREATE p = (" + subject.getLocalName() + ") -[:" + predicate.getLocalName() + "]->" + "(" + ((Resource) object).getLocalName() + ")"
//                    );
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            } else {
                if(rdfPropertyMaps.get(subject) == null) {
                    Document document = new Document("@id", subject.getURI());
                    rdfPropertyMaps.put(subject, document);
                }

                Document document = new Document();
                document.append("subject", subject.getURI());
                document.append("predict", predicate.getURI());
                document.append("object", object.toString());
                documents.add(document);
            }
        }

        System.out.println(mongoCollection.insertMany(documents));
    }

//    @GetMapping("/parse")
    public static void main(String[] args) {
        String sql = "select * { \n" +
                "    ?person <http://xmlns.com/foaf/0.1/name> ?name.\n" +
                "    ?person <http://xmlns.com/foaf/0.1/a> ?b\n" +
                "}";
        Query query = QueryFactory.create(sql);
//        query.isSelectType() && query.isQueryResultStar(); // of the form SELECT *?
        query.getDatasetDescription(); // FROM / FROM NAMED bits
        query.getQueryPattern(); // The meat of the query, the WHERE bit...etc etc..
        Op op = Algebra.compile(query); // Get the algebra for th
        System.out.println(op);
    }

    private Model getModelFromPath(String path)
    {
        InputStream in = RDFDataMgr.open(path);
        Model model = ModelFactory.createDefaultModel();
        model.read(in, null);
        return model;
    }
}
