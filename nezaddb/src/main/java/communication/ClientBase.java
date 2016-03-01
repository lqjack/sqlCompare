package communication;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient; //import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class ClientBase {
    private String host;
    private int port;
    private HttpClient httpclient;

    class ObjContentProducer implements ContentProducer {
        private Object content;
        
        public ObjContentProducer(Object content) {
            this.content = content;
        }

        public void writeTo(OutputStream out) throws IOException {
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(content);
        }
    }

    public ClientBase(String host, int port) {
        this.host = host;
        this.port = port;
        httpclient = new DefaultHttpClient();
    }

    public String sendFile(String ctx, File file, String name, String type) throws IOException {
        HttpPost httppost = new HttpPost(String.format("http://%s:%d/%s", host, port, ctx));
        httppost.setEntity(new FileEntity(file, type));
        httppost.addHeader("x-file", name);
        
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity entity = response.getEntity();
        
        if (entity != null) {
            BufferedReader bufReader = new BufferedReader(new InputStreamReader(entity.getContent()));
            String msg = bufReader.readLine();
            return msg;
        }
        return "fail";
    }
    
    public Object sendContext(String ctx, Object content) throws IOException {
    	Object result = null;
        ContentProducer objProducer = new ObjContentProducer(content);
        
        HttpPost httppost = new HttpPost(String.format("http://%s:%d/%s", host, port, ctx));
        httppost.setEntity(new EntityTemplate(objProducer));
        
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity entity = response.getEntity();
        
        String type;
        if (response.containsHeader("x-classtype")) 
            type = response.getLastHeader("x-classtype").getValue();
        // if headers does not indicate the type of the result , assume class as default
        else {
            type = "class";
        }

        if (entity != null) {
            InputStream instream = entity.getContent();
            ObjectInputStream ois = new ObjectInputStream(instream);
            try {
                if (type.equals("int")) {
                    result = ois.readInt();
                }
                if (type.equals("boolean")) {
                    result = ois.readBoolean();
                    System.out.println("receive boolean " + result);
                }
                if (type.equals("class")) {
                    result = ois.readObject();
                }
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
            ois.close();
            instream.close();
        }
        return result;
    }
    
    public Object sendContext(String ctx, Object content, HashMap<String, String> meta) throws IOException {
        Object result = null;
        ContentProducer objProducer = new ObjContentProducer(content);
        
        HttpPost httppost = new HttpPost(String.format("http://%s:%d/%s", host, port, ctx));
        httppost.setEntity(new EntityTemplate(objProducer));
        Set<String> metaData = meta.keySet();
        Iterator<String> it = metaData.iterator();
        while (it.hasNext()) {
            String name = it.next();
            if (httppost.getFirstHeader(name) == null) {
                httppost.addHeader(name, meta.get(name));
            }
        }
        
        HttpResponse response = httpclient.execute(httppost);
        HttpEntity entity = response.getEntity();
        
        String type;
        if (response.containsHeader("x-classtype")) 
            type = response.getLastHeader("x-classtype").getValue();
        // if headers does not indicate the type of the result , assume class as default
        else {
            type = "class";
        }

        if (entity != null) {
            InputStream instream = entity.getContent();
            ObjectInputStream ois = new ObjectInputStream(instream);
            try {
                if (type.equals("int")) {
                    result = ois.readInt();
                }
                if (type.equals("boolean")) {
                    result = ois.readBoolean();
                    System.out.println("receive boolean " + result);
                }
                if (type.equals("class")) {
                    result = ois.readObject();
                }
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
            ois.close();
            instream.close();
        }
        return result;
    }
    
    
    /*
    public static void main(String args[]) throws IOException {
        ClientBase client = new ClientBase("localhost", 9000);
        Object result = client.sendContext("createdb", "create database abc");
        boolean r = ((Boolean)result).booleanValue();
        if (r == true) {
            System.out.println("success!");
        }

    }
	*/
}
