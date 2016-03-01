package executeReturnResult;

import java.util.ArrayList;

public class TestSiteExecuteReturn extends ExecuteReturnResult {
    static final long serialVersionUID = 1;
    private  ArrayList<String> status = new ArrayList<String>();
    
    public ArrayList<String> getSiteStatus() {
        return status;
    }
    
    public void addStatus(String s) {
        status.add(s);
    }
    
    @Override
    public void displayResult() {
        // TODO Auto-generated method stub

    }

}
