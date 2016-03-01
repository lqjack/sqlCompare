package communication;

import dbcore.DbManager;
import dbcore.DbTable;
import executeResult.CreateDBExecuteResult;
import executeResult.CreateTableExecuteResult;
import executeResult.DeleteExecuteResult;
import executeResult.ImportExecuteResult;
import executeResult.ImportExecuteResultUnit;
import executeResult.InsertExecuteResult;
import executeResult.SelectExecuteResult;
import executeResult.SiteExecuteResult;
import gdd.GDD;
import globalDefinition.CONSTANT;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.sql.SQLException;
import java.util.Vector;

import queryTree.TreeNode;
import slaveExecute.DeleteRecordExecute;
import slaveExecute.SelectExecutePlan;
import slaveExecuteResult.DeleteBatchResult;
import slaveExecuteResult.GetAllTableResult;
import slaveExecuteResult.SlaveCreateTableResult;
import slaveExecuteResult.SlaveDeleteResult;
import slaveExecuteResult.SlaveImportResult;
import slaveExecuteResult.SlaveInsertResult;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import configuration.Configuration;

public class DataServer extends ServerBase {
    private GDD gdd;

    public DataServer() throws IOException {
        super();
        int defaultPort = Integer.parseInt(Configuration.getInstance()
                .getOption("SiteConfiguration.default.dataport"));
        setPort(defaultPort);
        initDataServer();
        gdd = GDD.getInstance();
        gdd.GDDReader("config/gdd.config");
    }

    public void initDataServer() {
        createContext("/gddfile", new GDDFileHandler());
        createContext("/createdb", new CreateDbHandler());
        createContext("/createtable", new CreateTableHandler());
        createContext("/importtable", new ImportTableHandler());
        createContext("/inserttable", new InsertTableHandler());
        createContext("/deletetable", new DeleteTableHandler());
        createContext("/getalltable", new GetAllTableHandler());
        createContext("/deletebatchtable", new DeleteBatchTableHandler());
        createContext("/selecttable", new SelectTableHandler());
        createContext("/cleardb", new ClearDbHandler());
        createContext("/clearcache", new ClearCacheHandler());
        createContext("/test", new TestHandler());
        
    }

    public static void main(String[] args) {
        try {
            DataServer server = new DataServer();
            server.start();
        } catch (IOException ex) {
            System.out.println("DataServer: " + ex.toString());
        }
    }

}

class TestHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        //ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
        //ois.close();
        Headers headers = t.getResponseHeaders();
        headers.add("x-classtype", "class");
        t.sendResponseHeaders(200, 0);
        ObjectOutputStream oos = new ObjectOutputStream(t.getResponseBody());
        oos.writeObject("ok");
        oos.close();
    }
}

class ClearCacheHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        Long session = null;
        ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
        try {
            session = (Long) ois.readObject();
        } catch (ClassNotFoundException ex) {
            
        }
        ois.close();
        GlobalQueryCache.getInstance().clearBuf(session);
        Headers headers = t.getResponseHeaders();
        headers.add("x-classtype", "class");
        t.sendResponseHeaders(200, 0);
        ObjectOutputStream oos = new ObjectOutputStream(t.getResponseBody());
        oos.writeObject("ok");
        oos.close();
    }
}

class SelectTableHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        try {
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
            SelectExecuteResult exeResult = (SelectExecuteResult) ois
                    .readObject();
            TreeNode node = exeResult.getRoot();
            ois.close();
            SelectExecutePlan exePlan = new SelectExecutePlan(exeResult);
            DbTable table = exePlan.execute(node);
            SiteExecuteResult result = new SiteExecuteResult(table, exePlan.getTransferSize());
            Headers headers = t.getResponseHeaders();
            headers.add("x-classtype", "class");
            t.sendResponseHeaders(200, 0);
            ObjectOutputStream oos = new ObjectOutputStream(t.getResponseBody());
            oos.writeObject(result);
            oos.close();
        } catch (ClassNotFoundException ex) {
            System.out.println("Dataserver: handle " + ex.toString());
        }
    }
}

class GDDFileHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        try {
            Headers reqHeaders = t.getRequestHeaders();
            if (!reqHeaders.containsKey("x-file"))
                return;
            String fileName = reqHeaders.getFirst("x-file");
            System.out.println("write file: " + fileName);
            InputStream input = t.getRequestBody();
            FileOutputStream fileOut = new FileOutputStream("config\\"
                    + fileName);
            byte[] buf = new byte[8192];
            int count;
            String msg = "ok";
            try {
                while ((count = input.read(buf, 0, 8192)) != -1) {
                    fileOut.write(buf, 0, count);
                }
            } catch (IOException ex) {
                msg = ex.toString();
            } finally {
                input.close();
                fileOut.close();
            }

            // file write finished, reload gdd information
            GDD.getInstance().GDDReader("config/gdd.config");

            t.sendResponseHeaders(200, 0);
            OutputStream os = t.getResponseBody();
            BufferedWriter bufWriter = new BufferedWriter(
                    new OutputStreamWriter(os));
            try {
                bufWriter.write(msg);
            } catch (IOException ex) {
                System.out.println("DataServer:GDDFileTransfer "
                        + ex.toString());
            } finally {
                bufWriter.close();
                os.close();
            }
        } catch (Exception ex) {
            System.out.println("DataServer:GDDFileTransfer " + ex.toString());
        }
    }
}

class CreateDbHandler implements HttpHandler {
    //private static final String CreateDBExecuteResult = null;
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        try {
            Headers headers = t.getResponseHeaders();
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
            // BufferedReader reader = new BufferedReader(new
            // InputStreamReader(t.getRequestBody()));
            // read the sql command in the request
            CreateDBExecuteResult cmd = (CreateDBExecuteResult) ois
                    .readObject();
            // String cmd = reader.readLine();
            // System.out.println(cmd);
            ois.close();

            headers.add("x-classtype", "boolean");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);

            DbManager dbm = new DbManager();

            System.out.println("receive request");
            boolean result = dbm.executeCreateDB(cmd.getSql());
            System.out.println("create database : " + result);
            oos.writeBoolean(result);
            // oos.writeObject(result);
        } catch (SQLException ex) {
            oos.writeBoolean(false);
            System.out.println("DataServer:Create DB " + ex.toString());
        } catch (Exception ex) {
            System.out.println("DataServer:Create DB " + ex.toString());
        } finally {
            oos.close();
            os.close();
        }
    }
}

class CreateTableHandler implements HttpHandler {
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        SlaveCreateTableResult createResult = null;
        try {
            Headers headers = t.getResponseHeaders();
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
            // read the sql command in the request
            CreateTableExecuteResult cmd = (CreateTableExecuteResult) ois
                    .readObject();
            ois.close();

            headers.add("x-classtype", "class");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);

            DbManager dbm = new DbManager();
            System.out.println("receive request");
            boolean result = true;

            for (int i = 0; i < cmd.getSql().size(); i++) {
                result &= dbm.executeCreateTable(cmd.getSql().elementAt(i));
            }

            System.out.println("create table : " + result);
            createResult = new SlaveCreateTableResult(true, null);
            oos.writeObject(createResult);
        } catch (Exception ex) {
            createResult = new SlaveCreateTableResult(false, ex.toString());
            oos.writeObject(createResult);
            System.out.println("DataServer:Create Table " + ex.toString());
        } finally {
            oos.close();
            os.close();
        }
    }
}

class ImportTableHandler implements HttpHandler {
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        SlaveImportResult importResult = null;
        try {
            Headers headers = t.getResponseHeaders();
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());

            ImportExecuteResult cmd = (ImportExecuteResult) ois.readObject();
            ois.close();

            headers.add("x-classtype", "class");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);

            DbManager dbm = new DbManager();
            System.out.println("receive request");
            // boolean result = true;

            String sql;
            ImportExecuteResultUnit unit;
            //int[] result;
            for (int i = 0; i < cmd.getImportUnits().size(); i++) {
                unit = cmd.getImportUnits().elementAt(i);
                Vector<String> sqls = new Vector<String>();
                for (int j = 0; j < unit.columnInfos.size(); j++) {
                    sql = "insert into " + unit.tableName
                            + unit.columnNameString + " values"
                            + unit.columnInfos.elementAt(j);
                    sqls.add(sql);
                }
                dbm.executeExecuteBatch(sqls);
            }

            importResult = new SlaveImportResult(true, null);
            oos.writeObject(importResult);
            // oos.writeBoolean(result);
        } catch (Exception ex) {
            importResult = new SlaveImportResult(false, ex.toString());
            oos.writeObject(importResult);
            System.out.println("DataServer:Import Data " + ex.toString());
            importResult.displayResult();
        } finally {
            // oos.writeObject(importResult);
            oos.close();
            os.close();
        }
        // oos.writeObject(importResult);
    }
}

class InsertTableHandler implements HttpHandler {
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        SlaveInsertResult insertResult;
        try {
            Headers headers = t.getResponseHeaders();
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
            InsertExecuteResult cmd = (InsertExecuteResult) ois.readObject();
            ois.close();

            headers.add("x-classtype", "class");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);

            DbManager dbm = new DbManager();

            boolean result = true;
            
            System.out.println(cmd.getInsertSql());
            int affected = dbm.executeInsert(cmd.getInsertSql());

            insertResult = new SlaveInsertResult(affected, result, null);

            oos.writeObject(insertResult);
        } catch (Exception ex) {
            insertResult = new SlaveInsertResult(0, false, ex.toString());
            oos.writeObject(insertResult);
            System.out.println("DataServer:Import Data " + ex.toString());
        } finally {
            oos.close();
            os.close();
        }
    }
}

class DeleteTableHandler implements HttpHandler {
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        SlaveDeleteResult deleteResult;
        try {
            Headers headers = t.getResponseHeaders();
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
            DeleteExecuteResult cmd = (DeleteExecuteResult) ois.readObject();
            ois.close();

            headers.add("x-classtype", "class");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);

            DbManager dbm = new DbManager();
            int affected = 0;
            int fragType = cmd.getFragType();
            switch (fragType) {
            case CONSTANT.FRAG_HORIZONTAL:
                affected = dbm.executeInsert(cmd.getDeleteSql());
                break;
            case CONSTANT.FRAG_VERTICAL:
            	DeleteRecordExecute deleteRecord = new DeleteRecordExecute(cmd);
                affected = deleteRecord.DeleteRecord();
                break;
            case CONSTANT.FRAG_HYBIRD:
                break;
            default:
            }

            deleteResult = new SlaveDeleteResult(affected, true, null);
            oos.writeObject(deleteResult);

        } catch (Exception ex) {
            deleteResult = new SlaveDeleteResult(0, false, ex.toString());
            oos.writeObject(deleteResult);
            System.out.println("DataServer:Delete table " + ex.toString());
        } finally {
            oos.close();
            os.close();
        }
    }
}

class GetAllTableHandler implements HttpHandler {
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        DbTable table = null;
        try {
            Headers headers = t.getResponseHeaders();
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
            GetAllTableResult cmd = (GetAllTableResult) ois.readObject();
            ois.close();

            headers.add("x-classtype", "class");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);

            DbManager dbm = new DbManager();

            table = dbm.executeSelect(cmd.getSql());

            oos.writeObject(table);

        } catch (Exception ex) {
            oos.writeObject(table);
            System.out.println("DataServer:Get All Table " + ex.toString());
        } finally {
            oos.close();
            os.close();
        }
    }
}

class DeleteBatchTableHandler implements HttpHandler {
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        SlaveDeleteResult deleteResult;
        try {
            Headers headers = t.getResponseHeaders();
            ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
            DeleteBatchResult cmd = (DeleteBatchResult) ois.readObject();
            ois.close();

            headers.add("x-classtype", "boolean");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);

            DbManager dbm = new DbManager();
            
            
            dbm.executeExecuteBatch(cmd.getSqls());

            oos.writeBoolean(true);

        } catch (Exception ex) {
            oos.writeBoolean(false);
            System.out.println("DataServer:Delete Batch table " + ex.toString());
        } finally {
            oos.close();
            os.close();
        }
    }
}

class ClearDbHandler implements HttpHandler {
    OutputStream os;
    ObjectOutputStream oos;

    public void handle(HttpExchange t) throws IOException {
        try {
            DbManager dbm = new DbManager();
            String dbname = Configuration.getInstance().getOption("SiteConfiguration.DatabaseConfiguration.dbname");
            dbm.dropDB(dbname);
            dbm.createDB(dbname);
            
            Headers headers = t.getResponseHeaders();
            headers.add("x-classtype", "class");
            t.sendResponseHeaders(200, 0);
            os = t.getResponseBody();
            oos = new ObjectOutputStream(os);
            oos.writeObject("ok");

        } catch (Exception ex) {
            System.out.println("DataServer: clear database " + ex.toString());
        } finally {
            oos.close();
            os.close();
        }
    }
}









