/**
 * Created with IntelliJ IDEA.
 * User: johnclark
 * Date: 9/18/13
 * Time: 9:50 AM
 * To change this template use File | Settings | File Templates.
 */

package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CAPIBehavior;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.UnavailableException;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

public class Neo4jCAPIBehavior implements CAPIBehavior {

    protected ObjectMapper mapper = new ObjectMapper();
    protected Logger logger;
    protected Semaphore activeRequests;

    static String cypherCreateCollection = "MERGE (col:Collection {id: {id} }) return col;";
    static String cypherCreateCoverage = "MERGE (cov:Coverage {id: {id} }) SET cov.from = {from}, cov.to = {to}, cov.lat = {lat}, cov.lon = {lon} return cov;";
    static String cypherCreateCovers = "Match (col:Collection), (cov:Coverage) WHERE col.id={colid} AND cov.id={covid} CREATE UNIQUE (col)-[rel:COVERS {id:{id}, from:{from}, to:{to}, tag:{tag}}]->(cov) return rel";
    static String cypherDeleteCovers = "Match (col:Collection)-[rel:COVERS]->(cov:Coverage) WHERE col.id={colid} AND cov.id={covid} AND NOT(rel.id IN {ids}) DELETE rel";
    static String cypherDeleteCoverages = "MATCH (col:Collection)-[rel:COVERS]->(cover:Coverage) WHERE col.id={colid} AND NOT(cover.id IN {ids}) DELETE rel,cover";

    public Neo4jCAPIBehavior(int maxConcurrentRequests, Logger logger) {
        this.activeRequests = new Semaphore(maxConcurrentRequests);
        this.logger = logger;
    }

    @Override
    public boolean databaseExists(String database) {
        String db = getElasticSearchIndexNameFromDatabase(database);
        if("collections".equals(db)|| "places".equals(db) || "repos".equals(db)) {
            return true;
        }

        return false;
    }

    @Override
    public Map<String, Object> getDatabaseDetails(String database) {
        if(databaseExists(database)) {
            Map<String, Object> responseMap = new HashMap<String, Object>();
            responseMap.put("db_name", getDatabaseNameWithoutUUID(database));
            return responseMap;
        }
        return null;
    }

    @Override
    public boolean createDatabase(String database) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public boolean deleteDatabase(String database) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public boolean ensureFullCommit(String database) {
        return true;
    }

    @Override
    public Map<String, Object> revsDiff(String database, Map<String, Object> revsMap) throws UnavailableException {

        // start with all entries in the response map
        Map<String, Object> responseMap = new HashMap<String, Object>();
        for (Entry<String, Object> entry : revsMap.entrySet()) {
            String id = entry.getKey();
            String revs = (String)entry.getValue();
            Map<String, String> rev = new HashMap<String, String>();
            rev.put("missing", revs);
            responseMap.put(id, rev);
        }

        return responseMap;
    }

    @Override
    public List<Object> bulkDocs(String database, List<Map<String, Object>> docs) throws UnavailableException {
        try {
            activeRequests.acquire();
        } catch (InterruptedException e) {
            throw new UnavailableException("Too many concurrent requests");
        }

        // keep a map of the id - rev for building the response
        Map<String,String> revisions = new HashMap<String, String>();

        List<Object> result = new ArrayList<Object>();

        for (Map<String, Object> doc : docs) {

            //logger.info("Bulk doc entry is "+ docs);

            // these are the top-level elements that could be in the document sent by Couchbase
            Map<String, Object> meta = (Map<String, Object>)doc.get("meta");
            Map<String, Object> json = (Map<String, Object>)doc.get("json");
            String base64 = (String)doc.get("base64");

            if(meta == null) {
                // if there is no meta-data section, there is nothing we can do
                logger.warn("Document without meta in bulk_docs, ignoring....");
                continue;
            } else {
                if ("non-JSON mode".equals(meta.get("att_reason"))) {
                    // optimization, this tells us the body isn't json
                    json = new HashMap<String, Object>();
                } else {
                    if (json == null && base64 != null) {
                        // no plain json, let's try parsing the base64 data
                        byte[] decodedData = Base64.decodeBase64(base64);
                        try {
                            // now try to parse the decoded data as json
                            json = (Map<String, Object>) mapper.readValue(decodedData, Map.class);
                        } catch (IOException e) {
                            logger.error("Unable to parse decoded base64 data as JSON, indexing stub for id: " + meta.get("id"));
                            logger.error("Body was: " + new String(decodedData) + " Parse error was: " + e);
                            json = new HashMap<String, Object>();

                        }
                    }
                }
            }

            // at this point we know we have the document meta-data
            // and the document contents to be indexed are in json

            String id = (String)meta.get("id");
            String rev = (String)meta.get("rev");

            //logger.info("Bulk doc entry is "+ json);

            Map<String, Object> itemResponse = new HashMap<String, Object>();
            itemResponse.put("id", id);
            itemResponse.put("rev", revisions.get(rev));
            result.add(itemResponse);

            boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;

            if(deleted) {

            } else {
                //create calls for batch
                System.out.println(""+id);

                List<Object> calls = new ArrayList<Object>();
                List<Object> newCalls;
                newCalls = neoCreateCollection(meta, json, calls.size());
                if(newCalls == null) {
                    continue;
                }
                calls.addAll(newCalls);
                newCalls = neoCreateCoverages(meta, json, calls.size());
                if(newCalls == null) {
                    continue;
                }
                calls.addAll(newCalls);
                try {
                    //make first batch call
                    String body =  mapper.writeValueAsString(calls);
                    //System.out.println(body);
                    String ret = executePost("http://localhost:7474/db/data/batch", body);
                    if(ret == null) {
                        continue;
                    }
                    //System.out.println(ret);

                    //System.out.println("");

                    //use return to index coverage nodes
                    List<Object> retjson = (List<Object>) mapper.readValue(ret, List.class);

                    //perform index calls
                    neoCallIndex(retjson);

                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }

        }


        activeRequests.release();

        return result;
    }



    private void neoCallIndex(List<Object> json) {

        for(Object ret : json) {
            Map<String, Object> body = (Map<String, Object>) ((Map<String, Object>) ret).get("body");
            List<Object> columns = (List<Object>) body.get("columns");
            if(columns.size() > 0 && ((String)columns.get(0)).equals("cov")) {

                //get self link
                List<Object> data = (List<Object>) body.get("data");
                List<Object> first = (List<Object>) data.get(0);
                Map<String, Object> firstObj = (Map<String, Object>) first.get(0);

                //create params block
                Map<String, Object> call = new HashMap<String, Object>();
                call.put("key","dummy");
                call.put("value","dummy");
                call.put("uri",(String)firstObj.get("self"));

                String callBody = null;
                try {
                    callBody = mapper.writeValueAsString(call);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
                //System.out.println(callBody);
                String indexRet = executePost("http://localhost:7474/db/data/index/node/geom",callBody);
                //System.out.println("");
                //System.out.println(indexRet);

            }
        }


        return;
    }

    private String executePost(String targetURL, String body) {
        return executePost(targetURL, body, 2);
    }

    private String executePost(String targetURL, String body, int retries) {
        URL url;
        HttpURLConnection connection = null;
        try {
            //Create connection
            url = new URL(targetURL);
            connection = (HttpURLConnection)url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent","curl/7.29.0");
            connection.setRequestProperty("Accept","application/json");
            connection.setRequestProperty("Content-Type","application/json");
            connection.setRequestProperty("Content-Length", "" +
                    Integer.toString(body.getBytes().length));
            //connection.setRequestProperty("Content-Language", "en-US");

            connection.setUseCaches (false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            //Send request
            DataOutputStream wr = new DataOutputStream (
                    connection.getOutputStream ());
            wr.writeBytes (body);
            wr.flush ();
            wr.close ();

            //Get Response
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            String line;
            StringBuffer response = new StringBuffer();
            while((line = rd.readLine()) != null) {
                response.append(line);
                //response.append('\n');
            }
            rd.close();
            return response.toString();

        } catch (Exception e) {

            if(retries > 0) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {

                }
                return executePost(targetURL, body, --retries);
            } else {
                System.out.println("unrecoverable error");
                //e.printStackTrace();
                System.out.println(body);
                return null;
            }

        } finally {

            if(connection != null) {
                connection.disconnect();
            }
        }
    }

    private List<Object> neoCreateCoverages(Map<String, Object> meta, Map<String, Object> json, int offset) {
        List<Object> calls = new ArrayList<Object>();

        //extract info
        String collectionId = (String)meta.get("id");
        Map<String, Object> coverages = (Map<String, Object>) json.get("coverage");
        Map<String, Object> contains = (Map<String, Object>) json.get("contains");

        //check for coverages and contains
        if(coverages == null || coverages.keySet().size() < 1 ||
                contains == null || contains.keySet().size() < 1) {
            return null;
        }

        //init offset for call ids
        int covOffset = offset;

        //init arrays to track covers and coverage
        List<Object> coverageIds = new ArrayList<Object>();
        List<Object> coversIds = new ArrayList<Object>();

        //create coverage nodes
        for (String coverageSha : coverages.keySet()) {

            String coverageId = (String)meta.get("id")+":"+coverageSha;
            coverageIds.add(coverageId);

            //create coverage node
            Map<String, Object> coverageParams = neoExtractCoverageParams(coverageId, (Map<String, Object>) coverages.get(coverageSha));
            calls.add(createCypherQuery(cypherCreateCoverage, coverageParams, covOffset++));

            //foreach contains, create covers relationship
            for (String containsSha : contains.keySet()) {

                String coversId = (String)meta.get("id")+":"+containsSha;
                coversIds.add(coversId);

                //create covers relationship
                Map<String, Object> coversParams = neoExtractCoversParams(collectionId, coverageId, coversId, (Map<String, Object>) contains.get(containsSha));
                calls.add(createCypherQuery(cypherCreateCovers, coversParams, covOffset++));
            }

            //remove any old contains covers relationships
            Map<String, Object> removeCoversParams = new HashMap<String, Object>();
            removeCoversParams.put("colid",collectionId);
            removeCoversParams.put("covid",coverageId);
            removeCoversParams.put("ids",coversIds);
            calls.add(createCypherQuery(cypherDeleteCovers, removeCoversParams, covOffset++));
        }

        //remove any old coverages
        Map<String, Object> removeCoverageParams = new HashMap<String, Object>();
        removeCoverageParams.put("colid",collectionId);
        removeCoverageParams.put("ids",coverageIds);
        calls.add(createCypherQuery(cypherDeleteCoverages, removeCoverageParams, covOffset++));

        return calls;
    }

    private Map<String, Object> neoExtractCoversParams(String collectionId, String coverageId, String coversId, Map<String, Object> contains) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("colid",collectionId);
        params.put("covid",coverageId);
        params.put("id",coversId);

        params.put("from",(Integer) contains.get("from"));
        params.put("to",(Integer) contains.get("to"));
        params.put("tag",(String) contains.get("tag"));

        return params;
    }

    private Map<String, Object> neoExtractCoverageParams(String coverageId, Map<String, Object> coverage) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("id",coverageId);
        params.put("from",(Integer) coverage.get("from"));
        params.put("to",(Integer) coverage.get("to"));

        Map<String, Object> geoJSON = (Map<String, Object>) coverage.get("geojson");
        if(((String)geoJSON.get("type")).equals("Point")) {
            List<Object> coordinates = (List<Object>) geoJSON.get("coordinates");
            Double lat;
            Double lon;
            try {
                lat = (Double) coordinates.get(0);
            } catch(ClassCastException e) {
                lat = Double.valueOf((Integer) coordinates.get(0));
            }
            try {
                lon = (Double) coordinates.get(1);
            } catch(ClassCastException e) {
                lon = Double.valueOf((Integer) coordinates.get(1));
            }
            params.put("lat",lat);
            params.put("lon",lon);
        }
        //TODO handle other types of geoJSON

        return params;
    }

    private List<Object> neoCreateCollection(Map<String, Object> meta, Map<String, Object> doc, int offset) {
        List<Object> calls = new ArrayList<Object>();

        //create the collection node
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("id",(String)meta.get("id"));

        calls.add(createCypherQuery(cypherCreateCollection, params, offset));

        return calls;
    }

    private Map<String, Object> createCypherQuery(String query, Map<String, Object> params, int id) {
        //create call body
        Map<String, Object> body = new HashMap<String, Object>();
        body.put("query",query);
        body.put("params",params);

        //create main call object
        Map<String, Object> call = new HashMap<String, Object>();
        call.put("method","POST");
        call.put("to","/cypher");
        call.put("id",id);
        call.put("body",body);

        return call;
    }

    @Override
    public Map<String, Object> getDocument(String database, String docId) {
        return getLocalDocument(database, docId);
    }

    @Override
    public Map<String, Object> getLocalDocument(String database, String docId) {
        return null; //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String storeDocument(String database, String docId, Map<String, Object> document) {
        return storeLocalDocument(database, docId, document);
    }

    @Override
    public String storeLocalDocument(String database, String docId, Map<String, Object> document) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getAttachment(String database, String docId, String attachmentName) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public String storeAttachment(String database, String docId, String attachmentName, String contentType, InputStream input) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public InputStream getLocalAttachment(String databsae, String docId, String attachmentName) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public String storeLocalAttachment(String database, String docId, String attachmentName, String contentType, InputStream input) {
        throw new UnsupportedOperationException("Attachments are not supported");
    }

    @Override
    public Map<String, Object> getStats() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    protected String getElasticSearchIndexNameFromDatabase(String database) {
        String[] pieces = database.split("/", 2);
        if(pieces.length < 2) {
            return database;
        } else {
            return pieces[0];
        }
    }

    protected String getDatabaseNameWithoutUUID(String database) {
        int semicolonIndex = database.indexOf(';');
        if(semicolonIndex >= 0) {
            return database.substring(0, semicolonIndex);
        }
        return database;
    }
}
