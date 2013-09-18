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
import java.util.List;
import java.util.Map;

public class Neo4jCAPIBehavior implements CAPIBehavior {

    @Override
    public boolean databaseExists(String database) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> getDatabaseDetails(String database) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean createDatabase(String database) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean deleteDatabase(String database) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean ensureFullCommit(String database) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> revsDiff(String database, Map<String, Object> revs) throws UnavailableException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Object> bulkDocs(String database, List<Map<String, Object>> docs) throws UnavailableException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> getDocument(String database, String docId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> getLocalDocument(String database, String docId) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String storeDocument(String database, String docId, Map<String, Object> document) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String storeLocalDocument(String database, String docId, Map<String, Object> document) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getAttachment(String database, String docId, String attachmentName) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String storeAttachment(String database, String docId, String attachmentName, String contentType, InputStream input) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getLocalAttachment(String databsae, String docId, String attachmentName) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String storeLocalAttachment(String database, String docId, String attachmentName, String contentType, InputStream input) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> getStats() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
