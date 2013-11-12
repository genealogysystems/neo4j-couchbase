/**
 * Created with IntelliJ IDEA.
 * User: johnclark
 * Date: 9/18/13
 * Time: 9:43 AM
 * To change this template use File | Settings | File Templates.
 */

package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CouchbaseBehavior;

import java.util.*;


public class Neo4jCouchbaseBehavior implements CouchbaseBehavior {

    protected String hostname;
    protected Integer port;

    public Neo4jCouchbaseBehavior(String hostname, Integer port) {
        this.hostname = hostname;
        this.port = port;
    }

    @Override
    public List<String> getPools() {
        /*
        As I understand it, always return the string "default"
         */
        //System.out.println("getPools");
        List<String> result = new ArrayList<String>();
        result.add("default");
        return result;
    }

    @Override
    public String getPoolUUID(String pool) {
        /*
        Always return the uuid using the pool name.
        If we ever want to support multiple neo4j clusters, we need to base this off of the unique cluster name
         */
        String ret = UUID.nameUUIDFromBytes(pool.getBytes()).toString().replace("-", "");
        //System.out.println("getPoolUUID");
        //System.out.println("in: "+pool);
        //System.out.println("out: "+ret);
        return ret;
    }

    @Override
    public Map<String, Object> getPoolDetails(String pool) {
        /*
        If pool is default, return buckets and nodes. else return null
         */
        if("default".equals(pool)) {
            Map<String, Object> bucket = new HashMap<String, Object>();
            bucket.put("uri", "/pools/" + pool + "/buckets?uuid=" + getPoolUUID(pool));

            Map<String, Object> responseMap = new HashMap<String, Object>();
            responseMap.put("buckets", bucket);

            List<Object> nodes = getNodesServingPool(pool);
            responseMap.put("nodes", nodes);

            //System.out.println("getPoolDetails");
            //System.out.println("in: "+pool);
            //System.out.println("out: "+responseMap);

            return responseMap;
        }
        return null;
    }

    @Override
    public List<String> getBucketsInPool(String pool) {
        /*
        We will always return the same buckets, enabling a map to collections, places, etc.
         */
        if("default".equals(pool)) {
            List<String> bucketNameList = new ArrayList<String>();

            bucketNameList.add("collections");
            bucketNameList.add("places");
            bucketNameList.add("repos");

            //System.out.println("getBucketsInPool");
            //System.out.println("in: "+pool);
            //System.out.println("out: "+bucketNameList);

            return bucketNameList;
        }
        return null;
    }

    @Override
    public String getBucketUUID(String pool, String bucket) {
        /*
        Only return if it is a bucket we actually have
         */
        if("default".equals(pool))  {
            if("collections".equals(bucket) || "places".equals(bucket) || "repos".equals(bucket)) {
                String ret = UUID.nameUUIDFromBytes(bucket.getBytes()).toString().replace("-", "");;
                //System.out.println("getBucketUUID");
                //System.out.println("in: "+pool);
                //System.out.println("in: "+bucket);
                //System.out.println("out: "+ret);
                return ret;
            }
            return null;
        }
        return null;
    }

    @Override
    public List<Object> getNodesServingPool(String pool) {
        /*
        There is only ever one node, the one that this plugin runs on.
         */
        if("default".equals(pool)) {
            List<Object> nodes = new ArrayList<Object>();

            Map<String, Object> nodePorts = new HashMap<String, Object>();
            nodePorts.put("direct", port);

            Map<String, Object> node = new HashMap<String, Object>();
            String hostPort = hostname + ":" + port.toString();
            node.put("couchApiBase", String.format("http://%s/", hostPort));
            node.put("hostname", hostPort);
            node.put("ports", nodePorts);
            nodes.add(node);

            //System.out.println("getNodesServingPool");
            //System.out.println("in: "+pool);
            //System.out.println("out: "+nodes);

            return nodes;
        }
        return null;
    }

    @Override
    public Map<String, Object> getStats() {
        //System.out.println("getStats");
        Map<String, Object> result = new HashMap<String, Object>();
        return result;
    }
}
