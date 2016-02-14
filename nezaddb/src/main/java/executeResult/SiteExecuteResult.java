package executeResult;

import dbcore.DbTable;

public class SiteExecuteResult extends ExecuteResult {
    private static final long serialVersionUID = 1L;
    private DbTable table = null;
    private long totalTransferSize = 0;
    
    public SiteExecuteResult(DbTable t, long size) {
        table = t;
        totalTransferSize = size;
    }
    
    public DbTable getTable () {
        return table;
    }
    
    public long getTransSize() {
        return totalTransferSize;
    }
    
    @Override
    public void displayResult() {
        // TODO Auto-generated method stub

    }

}
