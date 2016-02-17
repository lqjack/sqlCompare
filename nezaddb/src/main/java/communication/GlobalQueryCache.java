package communication;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import dbcore.DbManager;

public class GlobalQueryCache {
    private static GlobalQueryCache instance = null;
     
    private HashMap<Long, HashMap<Integer, BuffTable>> cache = null;
    private GlobalQueryCache () {
        cache = new HashMap<Long, HashMap<Integer, BuffTable>>();
        DbManager dbm = new DbManager();
        try {
            Iterator<ArrayList<Object>> it = dbm.executeSelect("show tables like 'session%'").getRows().iterator();
            while (it.hasNext()) {
                dbm.dropTable(it.next().get(0).toString());
            }
        } catch (SQLException ex){
            
        }
    }
    
    public static synchronized GlobalQueryCache getInstance() {
        if (instance == null) {
            instance = new GlobalQueryCache(); 
        }
        return instance;
    }
    
    public synchronized BuffTable getBuffTable(Long session, int cid) {
        if (cache.containsKey(session)) {
            HashMap<Integer, BuffTable> sessionBuf = cache.get(session);
            if(sessionBuf.containsKey(cid)) {
                return sessionBuf.get(cid);
            }
            else {
                sessionBuf.put(cid, new BuffTable());
                return sessionBuf.get(cid);
            }
        } else {
            HashMap<Integer, BuffTable> sessionBuf = new HashMap<Integer, BuffTable>();
            cache.put(session, sessionBuf);
            sessionBuf.put(cid, new BuffTable());
            return sessionBuf.get(cid);
        }
    }
    
    public synchronized void clearBuf(Long session) {
        if (cache.containsKey(session)) {
            HashMap<Integer, BuffTable> sessionBuf = cache.get(session);
            Iterator<BuffTable> it = sessionBuf.values().iterator();
            DbManager dbm = new DbManager();
            while (it.hasNext()) {
                try {
                    dbm.dropTable(it.next().getBuf());
                } catch (SQLException ex) {
                    System.out.println("GlobalQueryCache clear cache: " + ex.toString());
                }
            }
            cache.remove(session);
            dbm.closeDb();
        }
        return;
    }
}

