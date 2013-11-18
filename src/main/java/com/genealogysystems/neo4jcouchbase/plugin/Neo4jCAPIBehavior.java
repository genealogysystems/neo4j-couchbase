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

            //ignore checkpoint requests
            if(id.startsWith("_local/")) {
                continue;
            }

            boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;

            if(deleted) {
                try {
                    Map<String, Object> call = new HashMap<String, Object>();
                    call.put("id",(String) json.get("id"));
                    String callBody = mapper.writeValueAsString(call);
                    //System.out.println(callBody);

                    String delRet = executePost("http://localhost:7474/db/data/ext/CollectionIndexPlugin/graphdb/delete",callBody);
                    //System.out.println("");
                    //System.out.println(delRet);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            } else {
                //create calls for batch
                //System.out.println(""+id);


                //create and add geojson
                try {
                    //create call to index entry
                    Object geojsonObject = json.get("geojson");
                    //if geojson is null, continue
                    if(geojsonObject == null) {
                        //System.out.println("Found null in "+id);
                        continue;
                    }

                    Map<String, Object> call = new HashMap<String, Object>();
                    call.put("id",(String) json.get("id"));
                    call.put("repo_id",(String) json.get("repo_id"));
                    call.put("collection_id",(String) json.get("collection_id"));
                    call.put("from",(Integer) json.get("from"));
                    call.put("to",(Integer) json.get("to"));
                    call.put("tags",(ArrayList<String>) json.get("tags"));
                    String geojson =  mapper.writeValueAsString(geojsonObject);
                    call.put("geojson",geojson);

                    String callBody = mapper.writeValueAsString(call);

                    //System.out.println(callBody);

                    String indexRet = executePost("http://localhost:7474/db/data/ext/CollectionIndexPlugin/graphdb/index",callBody);
                    //System.out.println("");
                    //System.out.println(indexRet);
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }


            }

        }


        activeRequests.release();

        return result;
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
                System.out.println("Error");
                System.out.println(targetURL);
                System.out.println(body);
                e.printStackTrace();
                return null;
            }

        } finally {

            if(connection != null) {
                connection.disconnect();
            }
        }
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
