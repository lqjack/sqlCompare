package communication;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BuffTable {
    private Lock lock = new ReentrantLock();
    private String name = null;
    public boolean bufExist() {
        return (name != null);
    }
    
    public void lock() {
        lock.lock();
    }
    
    public void unlock() {
        lock.unlock();
    }
    
    public void setBuf(String n) {
        name = n;
    }
    
    public String getBuf() {
        return name;
    }
}
