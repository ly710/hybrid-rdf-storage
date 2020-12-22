package hybrid.rdf.storage;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.vocabulary.RDF;
import org.apache.jena.vocabulary.VCARD;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;

@RestController
public class Test {
    @GetMapping("/test")
    public void test() {
        Model model = ModelFactory.createDefaultModel();
        String rdfFilePath = "file:/Users/liyang/Downloads/politicianbill.owl.txt";
        // use the RDFDataMgr to find the input file
        InputStream in = RDFDataMgr.open(rdfFilePath);
        if (in == null) {
            throw new IllegalArgumentException("File: " + rdfFilePath + " not found");
        }

        model.read(in, null);

        Property property;
        VCARD.FN

        model.write(System.out);
    }
}
