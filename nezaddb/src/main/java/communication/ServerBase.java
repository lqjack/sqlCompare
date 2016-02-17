package communication;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.InetSocketAddress;
//import ddb.dbcore.*;

/*
 * Multi-threaded Http Server
 */
public class ServerBase {
    //private int port;
    private HttpServer server;
    public ServerBase() throws IOException {
        server = HttpServer.create();
    }
    
    public void setPort(int port) throws IOException {
        server.bind(new InetSocketAddress(port), port);
    }
    
    public void createContext(String path, HttpHandler handler) {
        server.createContext(path, new WrapperHandler(handler));
    }
    
    public void start() {
        server.start();
    }
    
    /*public static void main(String[] args) {
        try {
            ServerBase server = new ServerBase();
            server.createContext("/applications/myapp", new TestHandler());
            server.start();
        }
        catch (IOException ex) {
            System.out.println(ex.toString());
        }
    }*/
    
    private class WrapperHandler implements HttpHandler {
        private HttpHandler handler;
        
        /*public WrapperHandler() {
            handler = null;
        }*/
        
        public WrapperHandler(HttpHandler handler) {
            this.handler = handler;
        }
        
        public void handle(HttpExchange t) throws IOException {
            (new Thread(new HttpHandlerThread(t, handler))).start();
        }
    }
}

class HttpHandlerThread implements Runnable {
    private HttpExchange t;
    private HttpHandler handler;
    private static int count = 0;
    public HttpHandlerThread(HttpExchange t, HttpHandler handler) {
        this.t = t;
        this.handler = handler;
    }
    
    public void run() {
        try {
            count ++;
            handler.handle(t);
            System.out.println(count);
        }
        catch (IOException ex) {
            System.out.println(ex.toString());
        }
    }
}



