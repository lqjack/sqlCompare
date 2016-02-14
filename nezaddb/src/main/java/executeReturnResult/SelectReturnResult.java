package executeReturnResult;

import java.util.ArrayList;

public class SelectReturnResult extends ExecuteReturnResult {
    static final long serialVersionUID = 1;
    public long totalResponseTime = 0;
    public long totalTransferSize = 0;
    public ArrayList<ArrayList<Object>> rows = null;
    public ArrayList<String> colName = null;
    
    public SelectReturnResult(long time, long size, ArrayList<ArrayList<Object>> rs, ArrayList<String> col) {
        this.totalResponseTime = time;
        this.totalTransferSize = size;
        this.rows = rs;
        this.colName = col;
        isError = false;
    }
    
    public SelectReturnResult(String error) {
        isError = true;
        errorMsg = error;
    }
    
    @Override
    public void displayResult() {
        // TODO Auto-generated method stub

    }

}
