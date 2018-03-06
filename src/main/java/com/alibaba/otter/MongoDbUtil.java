package com.alibaba.otter;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import org.bson.Document;
import com.mongodb.client.MongoCollection;
import com.mongodb.BasicDBObject;

public class MongoDbUtil {

    private static String ACCOUNT = "";
    private static String PWD = "";
    private static String DatabaseName = "admin";
    private static String ADDR = "localhost"; // "127.0.0.1";
    private static int PORT = 27017; // 27017;

    private static MongoDatabase mongoDatabase = null;
    private static MongoClient mongoClient = null;

    static {
        try {

            /*
            ServerAddress serverAddress = new ServerAddress(ADDR,PORT);
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            addrs.add(serverAddress);

            MongoCredential credential = MongoCredential.createScramSha1Credential(ACCOUNT, DatabaseName, PWD.toCharArray());
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(credential);
            //通过连接认证获取MongoDB连接
            MongoClient mongoClient = new MongoClient(addrs,credentials);
*/
            // 连接到 mongodb 服务
            mongoClient = new MongoClient( ADDR , PORT );

            // 连接到数据库
            mongoDatabase = mongoClient.getDatabase(DatabaseName);

            System.out.println("Connect to database successfully");

        }
        catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }

    public static void DropTable(String databaseName, String tableName){
        mongoDatabase = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
        System.err.println(collection);
        collection.drop();
        System.err.println( "DropTable: " + tableName );
    }
/*
  // 不支持: 因為沒有變數可以取得，需要解析 sql statement, 懶得用~
    public static void AlterTable(String databaseName, String oldTableName, String newTableName){
        mongoDatabase = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(oldTableName);
        MongoNamespace newName = new MongoNamespace(databaseName ,newTableName);
        collection.renameCollection(newName);
        System.err.println( "RenameTable: " + oldTableName + " to " +   newTableName);
    }
*/

    public static void CreateTable(String databaseName, String tableName){
        mongoDatabase = mongoClient.getDatabase(databaseName);
        //MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
        mongoDatabase.createCollection(tableName);
        System.err.println( "CreateTable: " + tableName );
    }

    public static void MongoInsert(CanalEntry.Entry entry, List<CanalEntry.Column> columns) {
        String databaseName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        mongoDatabase = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
        Document document = new Document();
        for (CanalEntry.Column column : columns) {
            document.append(column.getName(), column.getValue());
        }
        try {
            collection.insertOne(document);
        } catch (Exception e) {
            System.err.println("Insert Row:  " + e.getMessage());
        }
    }

    public static void MongoDelete(CanalEntry.Entry entry, List<CanalEntry.Column> columns) {
        String databaseName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        mongoDatabase = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

        BasicDBObject document = new BasicDBObject();
        int index = 0;
        for (CanalEntry.Column column : columns) {
            if (!column.getIsKey())
                continue;

            document.put(column.getName(), column.getValue());
            index++;
        }

        try {
            if (index > 0)
                collection.deleteOne(document);
        } catch (Exception e) {
            System.err.println("Delete Row:  " + e.getMessage());
        }
    }

    public static void MongoUpdate(CanalEntry.Entry entry, List<CanalEntry.Column> beforeColumns, List<CanalEntry.Column> afterColumns)
    {
        String databaseName = entry.getHeader().getSchemaName();
        String tableName = entry.getHeader().getTableName();
        mongoDatabase = mongoClient.getDatabase(databaseName);
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
        Document newDocument = new Document();

        BasicDBObject andQuery = new BasicDBObject();
        List<BasicDBObject> obj = new ArrayList<BasicDBObject>();

        for (CanalEntry.Column column : afterColumns) {
            if(!column.getIsKey()) {
                newDocument.put(column.getName(), column.getValue());
            }
        }

        for (CanalEntry.Column column : beforeColumns) {
            if(column.getIsKey()) {
                newDocument.put(column.getName(), column.getValue());
                obj.add(new BasicDBObject(column.getName(), column.getValue()));
            }
        }

        andQuery.put("$and", obj);

        try {
            collection.replaceOne(andQuery, newDocument);
        }catch (Exception e){
            System.err.println( "Update Row:  " + e.getMessage());
        }
    }
}