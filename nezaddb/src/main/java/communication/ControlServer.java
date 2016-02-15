package communication;

import java.io.*;
import java.util.Set;
import java.util.Vector;

import com.sun.net.httpserver.*;

import execute.Execute;
import execute.GDDExecute;
import executeReturnResult.ExecuteReturnResult;
import executeReturnResult.GDDReturnResult;
import executeReturnResult.TestSiteExecuteReturn;
import gdd.GDD;
import gdd.SiteMeta;
import configuration.*;

public class ControlServer extends ServerBase {
    private GDD gdd;
    public ControlServer() throws IOException {
        super();
        gdd = GDD.getInstance();
        gdd.GDDReader("config/gddserver.config");
        int defaultPort = Integer.parseInt(Configuration.getInstance()
                .getOption("SiteConfiguration.default.ctrlport"));
        this.setPort(defaultPort);
        this.createContext("/executecmd", new SQLCommandHandler());
        this.createContext("/reset", new ResetHandler());
        this.createContext("/gddinfo", new GDDInfoHandler());
        this.createContext("/testsite", new TestSiteHandler());
    }

    
    public static void main(String[] args) {
        try {
            ControlServer server = new ControlServer();
            server.start();
        } catch (Exception ex) {
            System.out.println(ex.toString());
        }
    }
    
}

class TestSiteHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        TestSiteExecuteReturn result = new TestSiteExecuteReturn();
        Vector<SiteMeta> sites = GDD.getInstance().getSiteInfo();
        for (int i = 0; i < sites.size(); i++) {
            SiteMeta site = sites.get(i);
            try {
                ClientBase client = new ClientBase(site.getSiteIP(), site.getSitePort());
                String msg = (String)client.sendContext("test", "");
                if (msg.equals("ok"))
                    result.addStatus(site.getSiteName() + ": ok");
            } catch (Exception ex) {
                result.addStatus(site.getSiteName() + ": failed");
                System.out.println(ex.toString());
            }
        } 
        
        OutputStream os;
        ObjectOutputStream oos;
        Headers headers = t.getResponseHeaders();
        headers.add("x-classtype", "class");
        t.sendResponseHeaders(200, 0);
        os = t.getResponseBody();
        oos = new ObjectOutputStream(os);
        oos.writeObject(result);
        oos.close();
        os.close();
    }
}
class ResetHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        Vector<SiteMeta> sites = GDD.getInstance().getSiteInfo();
        for (int i = 0; i < sites.size(); i++) {
            SiteMeta site = sites.get(i);
            try {
                ClientBase client = new ClientBase(site.getSiteIP(), site.getSitePort());
                client.sendContext("cleardb", "");
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        } 
        File config = new File("config/gddserver.config");
        if (config.exists()) {
            config.delete();
        }
        
        OutputStream os;
        ObjectOutputStream oos;
        Headers headers = t.getResponseHeaders();
        headers.add("x-classtype", "class");
        t.sendResponseHeaders(200, 0);
        os = t.getResponseBody();
        oos = new ObjectOutputStream(os);
        oos.writeObject("ok");
        oos.close();
        os.close();
    }
}

class GDDInfoHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
    	
    	GDDExecute gddExecute = new GDDExecute();
    	gddExecute.genGDDInfos();
    	
        GDDReturnResult result = gddExecute.getResult();
        
        OutputStream os;
        ObjectOutputStream oos;
        Headers headers = t.getResponseHeaders();
        headers.add("x-classtype", "class");
        t.sendResponseHeaders(200, 0);
        os = t.getResponseBody();
        oos = new ObjectOutputStream(os);
        try {
            oos.writeObject(result);
        } catch (IOException ex) {
            throw ex;
        } finally {
            oos.close();
            os.close();
        }
        
    }
}


class SQLCommandHandler implements HttpHandler {
    public void handle(HttpExchange t) throws IOException {
        boolean fileTrans = false;
        boolean opt = true;
        boolean parseOnly = false;
        try {
            Execute exe = new Execute();
            Headers reqHeaders = t.getRequestHeaders();
            Set<String> set = reqHeaders.keySet();
            System.out.println(set.toString() + " " + set.size());
            
            if (reqHeaders.containsKey("x-file")) {
                fileTrans = true;
            }
            
            if (reqHeaders.containsKey("optimization")) {
                String option = reqHeaders.getFirst("optimization");
                if (option.equals("disable"))
                    opt = false;
                else
                    opt = true;
            }
            
            if (reqHeaders.containsKey("execution")) {
                String option = reqHeaders.getFirst("execution");
                if (option.equals("parse only"))
                    parseOnly = true;
                else
                    parseOnly = false;
            }
            
            Headers headers = t.getResponseHeaders();
            if (fileTrans == false) {
                
                 ObjectInputStream ois = new ObjectInputStream(t.getRequestBody());
                // BufferedReader reader = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                // read the sql command in the request
                String cmd = (String) ois.readObject();
                // String cmd = reader.readLine();
                System.out.println(cmd);

                ExecuteReturnResult rs;
                if (parseOnly) {
                    rs = exe.getParseTree(cmd, opt);
                }
                else {
                    exe.execute(cmd, opt);
                    rs = exe.getReturnResult();
                }

               // System.out.println(table.getRowCount());
                headers.add("x-classtype", "class");
                t.sendResponseHeaders(200, 0);
                OutputStream os = t.getResponseBody();
                ObjectOutputStream oos = new ObjectOutputStream(os);
                oos.writeObject(rs);
                oos.close();
                os.close();
            }
            else {
                String contentType = reqHeaders.getFirst("Content-Type");
                String fileName = reqHeaders.getFirst("x-file");
                if (contentType.equals("text/plain")) {
                    System.out.println("write file: " + fileName );
                    BufferedReader reader = new BufferedReader(new InputStreamReader(t.getRequestBody()));
                    FileWriter fileWriter = new FileWriter("upload\\" + fileName);
                    int count;
                    char [] buf = new char[8192];
                    while ((count = reader.read(buf, 0, 8192))!= -1) {
                        fileWriter.write(buf, 0, count);
                    }
                    reader.close();
                    fileWriter.close();
                    
                    // response
                    t.sendResponseHeaders(200, 0);
                    OutputStream os = t.getResponseBody();
                    BufferedWriter bufWriter = new BufferedWriter(new OutputStreamWriter(os));
                    bufWriter.write("ok");
                    bufWriter.close();
                    os.close();
                }
                // deal with binary file
                else {
                    System.out.println("write file: " + fileName );
                    InputStream input = t.getRequestBody();
                    FileOutputStream fileOut = new FileOutputStream("upload\\" + fileName);
                    byte [] buf = new byte[8192];
                    int count;
                    while ((count = input.read(buf, 0, 8192))!= -1) {
                        fileOut.write(buf, 0, count);
                    }
                    input.close();
                    fileOut.close();
                    
                    t.sendResponseHeaders(200, 0);
                    OutputStream os = t.getResponseBody();
                    BufferedWriter bufWriter = new BufferedWriter(new OutputStreamWriter(os));
                    bufWriter.write("ok");
                    bufWriter.close();
                    os.close();
                }
            }
        } catch (Exception ex) {
            System.out.println("ControlServer" + ex.toString());
        }
    }
}