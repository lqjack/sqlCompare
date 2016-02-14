package executeReturnResult;

public class ErrorReturnResult extends ExecuteReturnResult {
    static final long serialVersionUID = 1;
    String errorMsg;
    public ErrorReturnResult(String msg) {
        errorMsg = msg;
    }
    
    public String errorMsg() {
        return errorMsg;
    }
    
    @Override
    public void displayResult() {
        // TODO Auto-generated method stub
        System.out.println(errorMsg);
    }

}
