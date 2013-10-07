package com.genealogysystems.neo4jcouchbase.plugin;

import com.couchbase.capi.CAPIBehavior;
import com.couchbase.capi.CAPIServer;
import com.couchbase.capi.CouchbaseBehavior;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
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

    static int port = 8080;
    static String hostname = "localhost";
    static String username = "Administrator";
    static String password = "1gs234";

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel(Level.INFO);
        //System.out.println("Test");
        CouchbaseBehavior couchbaseBehavior = new Neo4jCouchbaseBehavior(hostname,port);
        CAPIBehavior capiBehavior = new Neo4jCAPIBehavior(32);

        CAPIServer capiServer = new CAPIServer(capiBehavior, couchbaseBehavior, port, username,password);
        capiServer.start();
    }
}
