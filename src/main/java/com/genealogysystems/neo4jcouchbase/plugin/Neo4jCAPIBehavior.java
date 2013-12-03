package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CAPIBehavior;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
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
        return "collections".equals(db) || "places".equals(db) || "repos".equals(db) || "entries".equals(db);

    }

    @Override
    public Map<String, Object> getDatabaseDetails(String database) {
        if(databaseExists(database)) {
            Map<String, Object> responseMap = new HashMap<>();
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
        Map<String, Object> responseMap = new HashMap<>();
        for (Entry<String, Object> entry : revsMap.entrySet()) {
            String id = entry.getKey();
            String revs = (String)entry.getValue();
            Map<String, String> rev = new HashMap<>();
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

        List<Object> result = new ArrayList<>();

        //Batch Delete
        ArrayList<String> toDelete = new ArrayList<>();

        //Batch Index
        ArrayList<String> indexEntityIDs = new ArrayList<>();
        ArrayList<String> indexRepositoryIDs = new ArrayList<>();
        ArrayList<String> indexCollectionIDs = new ArrayList<>();
        ArrayList<Integer> indexFroms = new ArrayList<>();
        ArrayList<Integer> indexTos = new ArrayList<>();
        ArrayList<String> indexTags = new ArrayList<>();
        ArrayList<String> indexGeojsons = new ArrayList<>();

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
                    json = new HashMap<>();
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
                            json = new HashMap<>();

                        }
                    }
                }
            }

            // at this point we know we have the document meta-data
            // and the document contents to be indexed are in json

            String id = (String)meta.get("id");
            //String rev = (String)meta.get("rev");

            //logger.info("Bulk doc entry is "+ json);

            Map<String, Object> itemResponse = new HashMap<>();
            itemResponse.put("id", id);
            itemResponse.put("rev", null); //not sure why null works here...
            result.add(itemResponse);

            //ignore checkpoint requests
            if(id.startsWith("_local/")) {
                continue;
            }

            boolean deleted = meta.containsKey("deleted") ? (Boolean)meta.get("deleted") : false;

            if(deleted) {
                toDelete.add(id);
            } else {
                Object geojsonObject = json.get("geojson");
                //if geojson is null, continue
                if(geojsonObject == null) {
                    //System.out.println("Found null in "+id);
                    continue;
                }
                try {
                    indexEntityIDs.add(id);
                    indexRepositoryIDs.add((String) json.get("repo_id"));
                    indexCollectionIDs.add((String) json.get("collection_id"));
                    indexFroms.add((Integer) json.get("from"));
                    indexTos.add((Integer) json.get("to"));
                    ArrayList<String> tags = (ArrayList<String>) json.get("tags");
                    indexTags.add(StringUtils.join(tags.toArray(), ','));
                    indexGeojsons.add(mapper.writeValueAsString(geojsonObject));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }

        } // End for loop

        //make delete request
        if(toDelete.size() > 0) {
            try {
                Map<String, Object> call = new HashMap<>();
                call.put("id",toDelete);

                String callBody = mapper.writeValueAsString(call);

                executePost("http://localhost:7474/db/data/ext/EntryIndexPlugin/graphdb/delete_batch",callBody);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //make index request
        if(indexEntityIDs.size() > 0) {
            try {
                Map<String, Object> call = new HashMap<>();
                call.put("id",indexEntityIDs);
                call.put("repo_id",indexRepositoryIDs);
                call.put("collection_id",indexCollectionIDs);
                call.put("from",indexFroms);
                call.put("to",indexTos);
                call.put("tags",indexTags);
                call.put("geojson",indexGeojsons);

                String callBody = mapper.writeValueAsString(call);

                executePost("http://localhost:7474/db/data/ext/EntryIndexPlugin/graphdb/index_batch",callBody);

            } catch (IOException e) {
                e.printStackTrace();
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
            StringBuilder response = new StringBuilder();
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
                    System.out.println("ERROR 500 - sleeping");
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

    @Override
    public Map<String, Object> getDocument(String database, String docId) {
        return getLocalDocument(database, docId);
    }

    @Override
    public Map<String, Object> getLocalDocument(String database, String docId) {
        return null;
    }

    @Override
    public String storeDocument(String database, String docId, Map<String, Object> document) {
        return storeLocalDocument(database, docId, document);
    }

    @Override
    public String storeLocalDocument(String database, String docId, Map<String, Object> document) {
        return null;
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
        return null;
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
