package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

/**
 * Created with IntelliJ IDEA.
 * User: johnclark
 * Date: 9/18/13
 * Time: 10:39 AM
 * To change this template use File | Settings | File Templates.
 */




public class Neo4jCouchbaseServer {

    static Logger logger = Logger.getLogger(Neo4jCouchbaseServer.class);

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();

        //System.out.println("Test");
        CouchbaseBehavior couchbaseBehavior = new Neo4jCouchbaseBehavior();
        CAPIBehavior capiBehavior = new Neo4jCAPIBehavior();

        CAPIServer capiServer = new CAPIServer(capiBehavior, couchbaseBehavior, "Administrator","1gs234");
        capiServer.start();
    }
}
