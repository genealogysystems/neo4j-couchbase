/**
 * Created with IntelliJ IDEA.
 * User: johnclark
 * Date: 9/18/13
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */

package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CouchbaseBehavior;

import java.util.List;
import java.util.Map;

public class Neo4jCouchbaseBehavior implements CouchbaseBehavior {

    @Override
    public List<String> getPools() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getPoolUUID(String pool) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> getPoolDetails(String pool) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<String> getBucketsInPool(String pool) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getBucketUUID(String pool, String bucket) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public List<Object> getNodesServingPool(String pool) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Map<String, Object> getStats() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
