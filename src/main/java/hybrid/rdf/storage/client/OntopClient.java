package hybrid.rdf.storage.client;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpAsQuery;
import org.apache.jena.sparql.algebra.TransformBase;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.springframework.stereotype.Service;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;

@Service
public class OntopClient {
    private static final String NULL_PREFIX = "urn:default:baseUri:";

    public ResultList find(Op op) {
        Op newOp = Transformer.transform(new OpTransfer(), op);
        String sparql = OpAsQuery.asQuery(newOp).serialize();
        QueryExecution queryExecution;

        try {
            queryExecution = QueryExecutionFactory.sparqlService(
                    "http://localhost:8080/sparql",
                    sparql
            );
        } catch (QueryParseException queryParseException) {
            return new ResultList();
        }

        ResultSet resultSet = queryExecution.execSelect();

        ResultList resultList = new ResultList();
        while(resultSet.hasNext()) {
            Map<String, Object> map = new HashMap<>();

            QuerySolution querySolution = resultSet.next();
            for (String resultVar : resultSet.getResultVars()) {
                String val = urlDecode(querySolution.get(resultVar).toString());
                if(val.startsWith(NULL_PREFIX)) {
                    val = val.replace(NULL_PREFIX, "");
                }
                map.put(resultVar, val);
            }

            resultList.add(map);
        }

        return resultList;
    }

    private static class OpTransfer extends TransformBase {
        @Override
        public Op transform(OpBGP opBGP) {
            BasicPattern originPattern = opBGP.getPattern();
            BasicPattern newPattern = new BasicPattern();

            for (Triple triple : originPattern) {
                Node subject = triple.getSubject();
                Node predict = triple.getPredicate();
                Node object = triple.getObject();

                if(!subject.isVariable()) {
                    subject = NodeFactory.createURI(NULL_PREFIX + urlEncode(subject.getURI()));
                }

                if(!predict.isVariable()) {
                    predict = NodeFactory.createURI(NULL_PREFIX + urlEncode(predict.getURI()));
                }

//                if(!object.isVariable()) {
//                    object = NodeFactory.createURI(NULL_PREFIX + urlEncode(object.getURI()));
//                }

                Triple newTriple = new Triple(subject, predict, object);
                newPattern.add(newTriple);
            }

            opBGP.getPattern().getList().removeAll(opBGP.getPattern().getList());
            opBGP.getPattern().addAll(newPattern);

            return opBGP;
        }

        private String urlEncode(String s) {
            try {
                return URLEncoder.encode(s, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();

                return "";
            }
        }
    }

    private String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return "";
        }
    }
}
