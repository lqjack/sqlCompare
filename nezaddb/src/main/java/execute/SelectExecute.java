package execute;

import java.io.IOException;
import java.util.Vector;

import parserResult.ParseResult;
import parserResult.SelectResult;
import queryTree.QueryTree;
import queryTree.TreeNode;

import communication.ClientBase;

import dbcore.DbTable;
import executeResult.SelectExecuteResult;
import executeResult.SiteExecuteResult;
import executeReturnResult.SelectReturnResult;
import gdd.GDD;
import gdd.SiteMeta;

public class SelectExecute extends ExecuteSQL {
    private SelectReturnResult selectReturnResult = null;
    //private static long uid = 0;
    private long uId;
    
    public SelectExecute() {
        uId = System.currentTimeMillis();
    }
    
    public SelectReturnResult getResult() {
        return selectReturnResult;
    }
    
    class ClearCacheJob implements Runnable {
        public void run() {
            Vector<SiteMeta> siteInfos = GDD.getInstance().getSiteInfo();
            for(int i = 0 ; i < siteInfos.size() ; i++){
                SiteMeta siteinfo = siteInfos.elementAt(i);
                ClientBase client = new ClientBase(siteinfo.getSiteIP(),siteinfo.getSitePort());
                System.out.println("clear cache@" + siteinfo.getSiteName() + " id " + uId);
                try {
                    client.sendContext("clearcache", new Long(uId));
                } catch (IOException ex) {
                    System.out.println("ClearCacheJob "+ ex.toString() );
                }
            }
        }
    }

    @Override
    public void execute(ParseResult result) {
        QueryTree tree = ((SelectResult) result).getSelectTree();
        TreeNode root = tree.getRoot();
        SiteMeta siteinfo = GDD.getInstance().getSiteInfo(root.getSiteID());
        int count = tree.getClassficationSize();
        SelectExecuteResult exeResult = new SelectExecuteResult(root, count, uId);
        ClientBase client = new ClientBase(siteinfo.getSiteIP(), siteinfo.getSitePort());
        try {
            long start = System.currentTimeMillis();
            SiteExecuteResult siteResult = (SiteExecuteResult)client.sendContext("selecttable", exeResult);
            DbTable table = siteResult.getTable();
            long size = siteResult.getTransSize();
            long end = System.currentTimeMillis();
            System.out.println("Total Rows: " + table.getRowCount() + " @" + (end - start) + "ms");
            selectReturnResult = new SelectReturnResult(end - start, size, table.getRows(), table.getColName());
            new Thread(new ClearCacheJob()).start();
        } catch (IOException ex) {
            selectReturnResult = new SelectReturnResult(ex.toString());
            System.out.println("SelectExecute: " + ex.toString());
        }
    }

}
