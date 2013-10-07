/**
 * Created with IntelliJ IDEA.
 * User: johnclark
 * Date: 9/18/13
 * Time: 9:50 AM
 * To change this template use File | Settings | File Templates.
 */

package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CAPIBehavior;

import javax.servlet.UnavailableException;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

public class Neo4jCAPIBehavior implements CAPIBehavior {

    protected Semaphore activeRequests;


    public Neo4jCAPIBehavior(int maxConcurrentRequests) {
        this.activeRequests = new Semaphore(maxConcurrentRequests);
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

        //TODO put noe4j code here

        activeRequests.release();

        return null;  //To change body of implemented methods use File | Settings | File Templates.
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
