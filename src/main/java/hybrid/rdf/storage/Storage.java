package hybrid.rdf.storage;

import hybrid.rdf.storage.config.Neo4jDataSourceConfig;
import org.apache.jena.rdf.model.*;
import org.apache.jena.riot.RDFDataMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;

@RestController
public class Storage {
    private final Neo4jDataSourceConfig.CypherExecutor cypherExecutor;

    @Autowired
    public Storage(Neo4jDataSourceConfig.CypherExecutor cypherExecutor) {
        this.cypherExecutor = cypherExecutor;
    }

    @GetMapping("/store")
    public void store(String path) {
        Model model = getModelFromPath("file:/home/ly/下载/politicianbill.owl");

        StmtIterator iter = model.listStatements();

        while (iter.hasNext()) {
            Statement stmt = iter.nextStatement();// get next statement
            Resource subject = stmt.getSubject();// 获得主体
            Property predicate = stmt.getPredicate();// 获得谓语
            //此处应注意!和java中Object要区分
            RDFNode object = stmt.getObject();// 获得客体!

//            System.out.print(subject.toString());
//            System.out.print(" " + predicate.toString() + " ");
            if (object instanceof Resource) {
                try {
                    cypherExecutor.execute(
                            "CREATE p = (" + subject.getLocalName() + ") -[:" + predicate.getLocalName() + "]->" + "(" + ((Resource) object).getLocalName() + ")"
                    );
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                // object is a literal
//                System.out.print(" \"" + object.toString() + "\"");
//                System.out.print(object.asLiteral().getDatatype());
            }

//            System.out.println(" .");
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
