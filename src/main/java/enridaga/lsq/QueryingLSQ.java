package enridaga.lsq;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.riot.Lang;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.sparql.engine.http.QueryExceptionHTTP;

public class QueryingLSQ {
	private int pageSize = 5000;
	
	
	
	private static String lsq = "http://lsq.aksw.org/sparql";
	private static Logger log = LoggerFactory.getLogger(QueryingLSQ.class);

	public Set<String> similar(String iri) {
		return exec(sim(iri,false), "X");
	}

	private String sim(String iri, boolean relaxed) {
		// lazy = consider predicates as variables, otherwise pick named iris only.
		Set<String> subjects = subjectVariables(iri);
		Set<String> objects = objectVariables(iri);
		Set<String> predicates = (relaxed) ? predicateVariables(iri) : namedPredicates(iri);
		Set<RDFNode> features = features(iri);
		StringBuilder sb = new StringBuilder(
				"PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp: <http://spinrdf.org/sp#> " + "SELECT DISTINCT ?X "
						+ " WHERE { <").append(iri).append("> "
								+ " a ?t ; lsqv:meanJoinVerticesDegree ?d ; lsqv:triplePatterns ?p ; lsqv:joinVertices ?j  ."
								+ "?X  a ?t ; lsqv:meanJoinVerticesDegree ?d ; lsqv:triplePatterns ?p ; lsqv:joinVertices ?j ; lsqv:resultSize ?r ");
		for (RDFNode s : features) {
			sb.append(" ; lsqv:usesFeature ").append('<').append(s.asResource().getURI()).append('>');
		}
		for (String s : subjects) {
			sb.append(" ; lsqv:mentionsSubject ").append('"').append(s).append('"');
		}
		for (String s : predicates) {
			sb.append(" ; lsqv:mentionsPredicate ");
			if(relaxed){
				// consider predicates as variables
				sb.append('"').append(s).append('"');
			}else{
				sb.append('<').append(s).append('>');
			}
		}
		for (String s : objects) {
			sb.append(" ; lsqv:mentionsObject ").append('"').append(s).append('"');
		}
		sb.append(" . filter(?r > 0) }");
		String sim = sb.toString();
		log.trace("Find equivalent queries: {}", sim);
		return sim;
	}

	private String mentionsVariables(String iri) {
		return new StringBuilder("PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp: <http://spinrdf.org/sp#> "
				+ "SELECT ?X WHERE {{ <").append(iri)
						.append("> lsqv:mentionsObject ?X . filter(strstarts(str(?X), \"?\")) } ").append("UNION")
						.append("{ <").append(iri)
						.append("> lsqv:mentionsPredicate ?X . filter(strstarts(str(?X), \"?\")) } ").append("UNION")
						.append("{ <").append(iri)
						.append("> lsqv:mentionsSubject ?X . filter(strstarts(str(?X), \"?\")) }}").toString();
	}

	private String mentionsSubjectVars(String iri) {
		return new StringBuilder(
				"PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp: <http://spinrdf.org/sp#> " + "SELECT ?X WHERE ")
						.append("{ <").append(iri)
						.append("> lsqv:mentionsSubject ?X . filter(strstarts(str(?X), \"?\")) }").toString();
	}

	private String mentionsPredicateVars(String iri) {
		return new StringBuilder(
				"PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp: <http://spinrdf.org/sp#> " + "SELECT ?X WHERE ")
						.append("{ <").append(iri)
						.append("> lsqv:mentionsPredicate ?X . filter(strstarts(str(?X), \"?\")) }").toString();
	}

	private String mentionsNamedPredicates(String iri) {
		return new StringBuilder(
				"PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp: <http://spinrdf.org/sp#> " + "SELECT ?X WHERE ")
						.append("{ <").append(iri)
						.append("> lsqv:mentionsPredicate ?X . filter(!strstarts(str(?X), \"?\")) }").toString();
	}

	private String mentionsObjectVars(String iri) {
		return new StringBuilder(
				"PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp: <http://spinrdf.org/sp#> " + "SELECT ?X WHERE ")
						.append("{ <").append(iri)
						.append("> lsqv:mentionsObject ?X . filter(strstarts(str(?X), \"?\")) }").toString();
	}

	private String usesFeature(String iri) {
		return new StringBuilder(
				"PREFIX lsqv:<http://lsq.aksw.org/vocab#> PREFIX sp: <http://spinrdf.org/sp#> " + "SELECT ?X WHERE ")
						.append("{ <").append(iri).append("> lsqv:usesFeature ?X }").toString();
	}

	public Set<RDFNode> features(String query) {
		return nodes(usesFeature(query), "X");
	}

	public Set<String> variables(String query) {
		return exec(mentionsVariables(query), "X");
	}

	public Set<String> subjectVariables(String query) {
		return exec(mentionsSubjectVars(query), "X");
	}

	public Set<String> predicateVariables(String query) {
		return exec(mentionsPredicateVars(query), "X");
	}

	public Set<String> namedPredicates(String query) {
		return exec(mentionsNamedPredicates(query), "X");
	}

	public Set<String> objectVariables(String query) {
		return exec(mentionsObjectVars(query), "X");
	}

	private String text(String iri) {
		return new StringBuilder("PREFIX lsqv:<http://lsq.aksw.org/vocab#>  PREFIX sp: <http://spinrdf.org/sp#> "
				+ "SELECT ?X " + " WHERE {" + "<").append(iri).append("> sp:text ?X . }").toString();
	}

	private String inSubjects(String iri) {
		return new StringBuilder("PREFIX lsqv:<http://lsq.aksw.org/vocab#> " + "PREFIX sp: <http://spinrdf.org/sp#> "
				+ "SELECT ?X" + " WHERE {" + "<").append(iri).append("> lsqv:mentionsSubject ?X . }").toString();
	}

	private String inPredicates(String iri) {
		return new StringBuilder("PREFIX lsqv:<http://lsq.aksw.org/vocab#> " + "PREFIX sp: <http://spinrdf.org/sp#> "
				+ "SELECT ?X" + " WHERE {" + "<").append(iri).append("> lsqv:mentionsPredicate ?X . }").toString();
	}

	private String inObjects(String iri) {
		return new StringBuilder("PREFIX lsqv:<http://lsq.aksw.org/vocab#> " + "PREFIX sp: <http://spinrdf.org/sp#> "
				+ " SELECT ?X" + " WHERE {" + "<").append(iri).append("> lsqv:mentionsObject ?X . }").toString();
	}

	private Set<String> exec(String qs, String v) {
		log.trace("{}", qs);
		Query q = QueryFactory.create(qs);
		Set<String> results = new HashSet<String>();
		boolean more = true;
		int offset = 0;
		int pageSize = this.pageSize;
		try {
			while (more) {
				q.setOffset(offset);
				q.setLimit(pageSize);
				QueryExecution execution = QueryExecutionFactory.sparqlService(lsq, q);
				ResultSet rs = execution.execSelect();
				int rn = 0;
				while (rs.hasNext()) {
					rn++;
					QuerySolution r = rs.next();
					RDFNode n = r.get(v);
					if (n == null) {

					} else if (n.isResource()) {
						results.add(r.getResource(v).getURI());
					} else if (n.isLiteral()) {
						results.add(r.getLiteral(v).getLexicalForm());
					} else if (n.isAnon()) {
						results.add(r.getResource(v).toString());// not sure...
					} else {
						log.error("Not the expected RDFNode: {}", n);
					}
				}

				if (rn < pageSize) {
					more = false;
				} else {
					offset += pageSize;
				}
				log.trace("Execution from offset: {}. Items: {}", offset, results.size());
			}
		} catch (QueryExceptionHTTP e) {
			log.error("HTTP: {}: {}", e.getResponseCode(), e.getResponseMessage());
		} catch (Exception e) {
			log.error("ERROR: ", e);
		}
		return results;
	}

	private Set<RDFNode> nodes(String qs, String v) {
		log.trace("{}", qs);
		Query q = QueryFactory.create(qs);
		Set<RDFNode> results = new HashSet<RDFNode>();
		boolean more = true;
		int offset = 0;
		int pageSize = this.pageSize;
		try {
			while (more) {
				q.setOffset(offset);
				q.setLimit(pageSize);
				QueryExecution execution = QueryExecutionFactory.sparqlService(lsq, q);
				ResultSet rs = execution.execSelect();
				int rn = 0;
				while (rs.hasNext()) {
					rn++;
					QuerySolution r = rs.next();
					RDFNode n = r.get(v);
					if (n != null) {
						results.add(n);
					}
				}

				if (rn < pageSize) {
					more = false;
				} else {
					offset += pageSize;
				}
				log.trace("Execution from offset: {}. Items: {}", offset, results.size());
			}
		} catch (Exception e) {
			log.error("ERROR: ", e);
		}
		return results;
	}

	public String qText(String query) {
		Set<String> s = exec(text(query), "X");
		if (s.size() == 1) {
			return s.iterator().next();
		}
		return "";
	}

	public Map<String, RDFNode> rewritings(String q1, String q2) {
		Set<RDFNode> q1s = nodes(inSubjects(q1), "X");
		Set<RDFNode> q2s = nodes(inSubjects(q2), "X");
		q1s.removeAll(q2s);
		Set<RDFNode> q1p = nodes(inPredicates(q1), "X");
		Set<RDFNode> q2p = nodes(inPredicates(q2), "X");
		q1p.removeAll(q2p);
		Set<RDFNode> q1o = nodes(inObjects(q1), "X");
		Set<RDFNode> q2o = nodes(inObjects(q2), "X");
		q1o.removeAll(q2o);
		Set<RDFNode> rewritable = new HashSet<RDFNode>();
		rewritable.addAll(q1s);
		rewritable.addAll(q1p);
		rewritable.addAll(q1o);
		Map<String, RDFNode> rewritings = new HashMap<String, RDFNode>();
		Map<String, String> datatypes = new HashMap<String, String>();
		int varn = 1;
		int prefn = 1;
		for (RDFNode n : rewritable) {
			StringBuilder varb = new StringBuilder("?_").append("param").append(varn);
			if (n.isAnon()) {
				// Skip
			} else if (n.isResource()) {
				varb.append("_iri");
			} else if (n.isLiteral()) {
				Literal l = n.asLiteral();
				if (!l.getLexicalForm().startsWith("?")) {
					// XXX Ignore if it is a variable ...
					if (l.getDatatype() != null) {
						// Only consider XSD datatypes
						String dt = l.getDatatype().getURI();
						String ns = (dt.indexOf('#') != -1) ? dt.substring(0, dt.indexOf('#') + 1)
								: dt.substring(0, dt.lastIndexOf('/') + 1);
						String ln = (dt.indexOf('#') != -1) ? dt.substring(dt.indexOf('#') + 1)
								: dt.substring(dt.lastIndexOf('/') + 1);
						String prefix = "p".concat(Integer.toString(prefn));
						datatypes.put(prefix, ns);
						prefn++;
						varb.append("_").append(prefix).append("_").append(ln);
					} else if (l.getLanguage() != null && !l.getLanguage().equals("")) {
						varb.append("_").append(l.getLanguage());
					}
				}
			}
			varn++;
			String var = varb.toString();
			rewritings.put(var, n);
			// log.error("rewriting: {} {}", var, n);
		}
		Set<String> query = exec(text(q1), "X");
		if(!query.isEmpty()){
			log.debug("Query: {}", query.iterator().next());
			for (Entry<String, RDFNode> en : rewritings.entrySet()) {
				log.debug(" {} -> {}", en.getValue().toString(), en.getKey());
			}
		}

		return Collections.unmodifiableMap(rewritings);
	}

	public Set<String> listQueries() {
		String qs = "PREFIX lsqv: <http://lsq.aksw.org/vocab#> SELECT ?X WHERE {?X lsqv:endpoint [] ; lsqv:resultSize ?r . filter(?r > 0) . } ";
		return exec(qs, "X");
	}

	public Set<String> listQueries(String endpoint) {
		String qs = "PREFIX lsqv: <http://lsq.aksw.org/vocab#> SELECT ?X WHERE {?X lsqv:endpoint <" + endpoint + ">  ; lsqv:resultSize ?r .  filter(?r > 0) . } ";
		return exec(qs, "X");
	}

	/**
	 * For each query (uri) in the set, generate an abstraction.
	 * Save it with pointers to all the abstracted queries.
	 * 
	 * @param queries
	 * @param fileName
	 * @throws IOException
	 */
	public static void generateAbstracted(Set<String> queries, String fileName) throws IOException {
		QueryingLSQ lsq = new QueryingLSQ();
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true));
		BufferedWriter sw = new BufferedWriter(new FileWriter(fileName + ".sim", true));
		String Sep = "=====";
		while (!queries.isEmpty()) {
			log.info("{} queries remaining", queries.size());
			String query = queries.iterator().next();
			log.debug("Examining query: {}", query);
			Set<String> similar = lsq.similar(query);
			// Save similar
			sw.write(query);
			for (String s : similar) {
				sw.write(' ');
				sw.write(s);
			}
			sw.newLine();
			sw.flush();

			queries.remove(query);
			queries.removeAll(similar);

//			if (similar.size() < 10)
//				continue;

			log.debug(" > Similar queries: {}", similar.size(), queries.size());
			String other = null;
			for (String s : similar) {
				if (!s.equals(query)) {
					other = s;
					break;
				}
			}
			bw.write(Sep);
			bw.write(query);
			bw.write(Sep);
			bw.write(other);
			bw.write(Sep);
			bw.write(lsq.qText(query));
			bw.write(Sep);
			bw.write(Integer.toString(similar.size()));
			bw.write(Sep);
			if (similar.size() != 0) {
				Map<String, RDFNode> rewritings = lsq.rewritings(query, other);
				for (Entry<String, RDFNode> r : rewritings.entrySet()) {
					bw.write(r.getValue().toString());
					bw.write(',');
					bw.write(r.getKey());
					bw.write(";");
				}
			} else {
				bw.write("NULL");
			}
			bw.newLine();
			bw.flush();
		}
		bw.close();
		sw.close();
	}
	
	public static void generateAbstractedRDF(Set<String> queries, String fileName) throws IOException {
		String bns = "http://basil.kmi.open.ac.uk/schema/";
		QueryingLSQ lsq = new QueryingLSQ();
		FileWriter bw = new FileWriter(fileName, true);
		while (!queries.isEmpty()) {
			Model m = ModelFactory.createDefaultModel();
			log.info("{} queries remaining", queries.size());
			String query = queries.iterator().next();
			log.debug("Examining query: {}", query);
			Set<String> similar = lsq.similar(query);
			for (String s : similar) {
				m.add(m.createResource(s), m.createProperty(bns + "prototype"), m.createResource(query));
			}
			queries.remove(query);
			queries.removeAll(similar);

			log.debug(" > Similar queries: {}", similar.size(), queries.size());
			String other = null;
			for (String s : similar) {
				if (!s.equals(query)) {
					other = s;
					break;
				}
			}
			// query, other
			m.add(m.createResource(query), m.createProperty(bns + "text"), lsq.qText(query));
			m.add(m.createResource(query), m.createProperty(bns + "comparedWith"), m.createResource(other));
			m.add(m.createResource(other), m.createProperty(bns + "text"), lsq.qText(other));
			m.add(m.createResource(query), m.createProperty(bns + "similars"), Integer.toString(similar.size()));
			if (similar.size() != 0) {
				Map<String, RDFNode> rewritings = lsq.rewritings(query, other);
				for (Entry<String, RDFNode> r : rewritings.entrySet()) {
					Resource re = m.createResource();
					re.addProperty(m.createProperty(bns + "prototypeValue"), r.getValue());
					re.addLiteral(m.createProperty(bns + "variable"), r.getKey());
				}
			}
			
			m.write(bw,Lang.NTRIPLES.getLabel());
		}

		
		bw.close();
	}

	/**
	 * [Operator] Generates a file with the list of queries (uris) of a given endpoint
	 * @param endpoint
	 * @param fileName
	 * @throws IOException
	 */
	public static void generateQueries(String endpoint, String fileName) throws IOException {
		QueryingLSQ lsq = new QueryingLSQ();
		Set<String> queries = lsq.listQueries(endpoint);
		BufferedWriter bw0 = new BufferedWriter(new FileWriter(fileName, true));
		Iterator<String> i = queries.iterator();
		int f = 0;
		while (i.hasNext()) {
			bw0.write(i.next());
			bw0.newLine();
			f++;
			if (f > 100) {
				bw0.flush();
				f = 0;
			}
		}
		bw0.close();
	}

	/**
	 * Loads a list of queries (uris) from a file
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public static Set<String> readQueries(String fileName) throws IOException {
		Set<String> queries = new HashSet<String>();
		FileReader rd = new FileReader(fileName);
		try (BufferedReader br = new BufferedReader(rd)) {
			String line;
			while ((line = br.readLine()) != null) {
				// process the line.
				queries.add(line);
			}
		}
		log.info("{} queries read", queries.size());
		return queries;
	}

	public static void main(String[] args) throws IOException {
		log.info("Starting");
		// 1. Generate the list of queries for the endpoint (can comment out if you already did it!)
		log.info("Saving the query list from the endpoint");
		generateQueries("http://dbpedia.org/sparql", "dbpedia-queries.dat");
		// Reading queries
		log.info("Loading query list");
		Set<String> queries = readQueries("dbpedia-queries.dat");
		log.info("Queries: {}", queries.size());
		String outfile = "dbpedia-abstracted.nt";
		log.info("Generating abstractions, writing to {}", outfile);
		generateAbstractedRDF(queries, outfile);
	}
}
