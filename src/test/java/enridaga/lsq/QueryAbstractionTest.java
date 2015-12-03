package enridaga.lsq;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.rdf.model.RDFNode;

public class QueryAbstractionTest {

	private static Logger log = LoggerFactory.getLogger(QueryAbstractionTest.class);
	private QueryingLSQ lsq = new QueryingLSQ();

	public void processTest() {
		Set<String> queries = lsq.listQueries();
		log.error("{} queries found", queries.size());
	}

	@Test
	public void similar() {
		String iri = "http://lsq.aksw.org/res/DBpedia-q275676";
		Set<String> similar = lsq.similar(iri);
		log.info("{} similar queries found", similar.size());
	}

	@Test
	public void testGenerateAbstract() {
		String q1 = "http://lsq.aksw.org/res/DBpedia-q432821";
		String q2 = "http://lsq.aksw.org/res/DBpedia-q431918";
		Map<String, RDFNode> r = lsq.rewritings(q1, q2);
		for (Entry<String, RDFNode> e : r.entrySet()) {
			log.info(" rewriting: {} {}", e.getKey(), e.getValue());
		}
	}

	@Test
	public void mentionVariables() {
		Set<String> vars = lsq.variables("http://lsq.aksw.org/res/DBpedia-q432821");
		for (String s : vars) {
			log.info("var: {}", s);
		}
	}
	
	
}
