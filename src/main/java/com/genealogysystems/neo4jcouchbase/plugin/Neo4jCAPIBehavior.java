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
import org.apache.log4j.Logger;

import org.codehaus.jackson.map.ObjectMapper;

import javax.servlet.UnavailableException;

import java.io.IOException;
import java.io.InputStream;
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

            boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;

            if(deleted) {

            } else {
                List<Object> calls = new ArrayList<Object>();
                calls.addAll(neoCreateCollection(meta, json, calls.size()));
                calls.addAll(neoCreateCoverages(meta, json, calls.size()));
                try {
                    System.out.println(mapper.writeValueAsString(calls));
                } catch (IOException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }


            Map<String, Object> itemResponse = new HashMap<String, Object>();
            itemResponse.put("id", id);
            itemResponse.put("rev", revisions.get(rev));
            result.add(itemResponse);
        }

        //TODO put neo4j code here


        activeRequests.release();

        return result;
    }

    private List<Object> neoCreateCoverages(Map<String, Object> meta, Map<String, Object> json, int offset) {

        List<Object> calls = new ArrayList<Object>();

        Map<String, Object> coverages = (Map<String, Object>) json.get("coverage");
        Map<String, Object> contains = (Map<String, Object>) json.get("contains");

        for (String coverageSha : coverages.keySet()) {

            String covId = (String)meta.get("id")+":"+coverageSha;
            int covOffset = offset;

            Map<String, Object> coverage = (Map<String, Object>) coverages.get(coverageSha);
            Integer from = (Integer) coverage.get("from");
            Integer to = (Integer) coverage.get("to");
            Map<String, Object> geoJSON = (Map<String, Object>) coverage.get("geojson");
            if(!((String)geoJSON.get("type")).equals("Point")) {
                continue;
            }
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

            //create coverage node
            Map<String, Object> createCoverage = new HashMap<String, Object>();
            createCoverage.put("method","POST");
            createCoverage.put("to","/index/node/coverage?uniqueness=get_or_create");
            createCoverage.put("id",offset++);
            Map<String, Object> body = new HashMap<String, Object>();
            body.put("key","cov:id");
            body.put("value",covId);
            Map<String, Object> properties = new HashMap<String, Object>();
            properties.put("cov:id",covId);
            properties.put("lat",lat);
            properties.put("lon",lon);
            body.put("properties",properties);
            createCoverage.put("body",body);
            calls.add(createCoverage);

            //create label call
            /*
            Map<String, Object> createLabel = new HashMap<String, Object>();
            createLabel.put("method","POST");
            createLabel.put("to","{"+covOffset+"}/labels");
            createLabel.put("id",offset++);
            createLabel.put("body","coverage");
            calls.add(createLabel);
            */
            //create relationships
            for (String containsSha : contains.keySet()) {

                String conId = (String)meta.get("id")+":"+coverageSha;

                Map<String, Object> contain = (Map<String, Object>) coverages.get(coverageSha);
                Integer conFrom = (Integer) contain.get("from");
                Integer conTo = (Integer) contain.get("to");
                String tag = (String) contain.get("tag");


                Map<String, Object> createRel = new HashMap<String, Object>();
                createRel.put("method","POST");
                createRel.put("to","{0}/relationships");
                createRel.put("id",offset++);
                Map<String, Object> relBody = new HashMap<String, Object>();
                relBody.put("to","{"+covOffset+"}");
                relBody.put("value",covId);
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("from",conFrom);
                data.put("to",conTo);
                data.put("tag",tag);
                relBody.put("data",data);
                createRel.put("body",relBody);
                calls.add(createRel);
            }

        }


        return calls;
    }

    private List<Object> neoCreateCollection(Map<String, Object> meta, Map<String, Object> doc, int offset) {
        List<Object> calls = new ArrayList<Object>();

        //create the collection node
        Map<String, Object> createCollection = new HashMap<String, Object>();
        createCollection.put("method","POST");
        createCollection.put("to","/index/node/collection?uniqueness=get_or_create");
        createCollection.put("id",offset);
        Map<String, Object> body = new HashMap<String, Object>();
        body.put("key","col:id");
        body.put("value",(String)meta.get("id"));
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("col:id",(String)meta.get("id"));
        body.put("properties",properties);
        createCollection.put("body",body);
        calls.add(createCollection);

        //create label call
        /*
        Map<String, Object> createLabel = new HashMap<String, Object>();
        createLabel.put("method","POST");
        createLabel.put("to","{"+offset+"}/labels");
        createLabel.put("id",offset+1);
        createLabel.put("body","collection");
        calls.add(createLabel);
        */
        return calls;
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
