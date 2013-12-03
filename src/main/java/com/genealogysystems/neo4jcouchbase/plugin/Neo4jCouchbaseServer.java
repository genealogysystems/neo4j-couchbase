package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;



public class Neo4jCouchbaseServer {

    static Logger logger = Logger.getLogger(Neo4jCouchbaseServer.class);

    static int port = 9091;
    static String hostname = "node2.neo4j.far.genealogysystems.com";
    static String username = "Administrator";
    static String password = "1gs234";

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);

        CouchbaseBehavior couchbaseBehavior = new Neo4jCouchbaseBehavior(hostname,port);
        CAPIBehavior capiBehavior = new Neo4jCAPIBehavior(16, logger);

        CAPIServer capiServer = new CAPIServer(capiBehavior, couchbaseBehavior, port, username,password);
        capiServer.start();
    }
}
