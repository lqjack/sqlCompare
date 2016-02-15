package slaveExecute;

import gdd.ColumnInfo;
import gdd.GDD;
import gdd.SiteMeta;
import gdd.TableMeta;
import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import queryTree.JoinNode;
import queryTree.LeafNode;
import queryTree.ProjectionNode;
import queryTree.SelectionNode;
import queryTree.TreeNode;
import queryTree.UnionNode;

import communication.BuffTable;
import communication.ClientBase;
import communication.GlobalQueryCache;

import configuration.Configuration;
import dbcore.DbColumn;
import dbcore.DbManager;
import dbcore.DbTable;
import executeResult.SelectExecuteResult;
import executeResult.SiteExecuteResult;

public class SelectExecutePlan {
    protected ArrayList<String> attrNameList = new ArrayList<String>();
    protected ArrayList<SimpleExpression> exprList = new ArrayList<SimpleExpression>();
    protected int siteId;
    protected long uid;
    protected int classesCount;
    protected String [] tableBuffer = null;
    protected Lock [] bufLock = null;
    protected long transferSize = 0;
    protected DbManager dbm = null;
    
    class JoinTask implements Runnable {
        private String [] table = null;
        private int index = 0;
        private final CountDownLatch barrier;
        private TreeNode node = null;
        private String primaryKey = null;
        public JoinTask(TreeNode n, CountDownLatch latch, String [] t, int idx, String key) {
            node = n;
            barrier = latch;
            table = t;
            index = idx;
            primaryKey = key;
        }
        
        public void run() {
            BuffTable buf = GlobalQueryCache.getInstance().getBuffTable(uid, node.getClassID());
            buf.lock();
            if (buf.bufExist()) {
                buf.unlock();
                System.out.println("@@@@@@@@@@@@@@@@hit" + node.getNodeID());
                table[index] = buf.getBuf();
            }
            /*bufLock[node.getClassID()].lock();
            if (tableBuffer[node.getClassID()] != null) {
                bufLock[node.getClassID()].unlock();
                System.out.println("@@@@@@@@@@@@@@@@hit" + node.getNodeID());
                table[index] = tableBuffer[node.getClassID()];
            }*/
            else if (node.getSiteID() == siteId) {
                LocalSelectExecutePlan localExe = new LocalSelectExecutePlan(
                        dbm, null, tableBuffer, node.getNodeID(), bufLock, uid);                    
                table[index] = localExe.executeLocal(node);
                transferSize += localExe.transferSize;
                //tableBuffer[node.getClassID()] = table[index];
                //bufLock[node.getClassID()].unlock();
                buf.setBuf(table[index]);
                buf.unlock();
                
            } else {
                DbTable fragment = sendExecutionPlan(node);
                String[] attrList = null;
                attrList = new String[fragment.getColName().size()];
                attrList = fragment.getColName().toArray(attrList);
                String[] types = null;
                String tableName = getTempName(node.getNodeID());
                //tableBuffer[node.getClassID()] = tableName;
                buf.setBuf(tableName);
                table[index] = tableName;

                types = new String[attrList.length];
                int key = fragment.getKey();
                for (int j = 0; j < attrList.length; j++) {
                    String[] colInfo = attrList[j].split("_", 2);
                    ColumnInfo info = GDD.getInstance().getColumnInfo(
                            colInfo[0], colInfo[1]);
                    types[j] = info.getColumnTypeName();
                    if (primaryKey != null && primaryKey.equals(attrList[j])) {
                        types[j] += " primary key ";
                    } else if (key == j) {
                        types[j] += " unique key ";
                        System.out.println(tableName + " has key");
                    }
                }
                // types = fragment.getTableName().toArray(types);
                try {
                    dbm.createTempTable(tableName, attrList, types);
                } catch (SQLException ex) {
                    System.out.println("DataServer: Join " + ex.toString());
                }

                try {
                    dbm.importData(tableName, attrList, fragment
                            .getRows());
                } catch (SQLException ex) {
                    System.out.println("DataServer: Join " + ex.toString());
                }
                fragment = null;
                System.gc();
                //bufLock[node.getClassID()].unlock();
                buf.unlock();
            }
            barrier.countDown();
        }
    }

    public SelectExecutePlan() {
        siteId = getLocalSiteID();
    }
    
    public String getTempName(int nodeID) {
        return "session" + uid + "_temp" + nodeID;
    }
    
    public SelectExecutePlan(SelectExecuteResult result) {
        siteId = getLocalSiteID();
        dbm = new DbManager();
        classesCount = result.getClassesCount();
        uid = result.getUID();
        tableBuffer = new String[classesCount];
        bufLock = new Lock[classesCount];
        for (int i = 0; i < classesCount; i ++)
            bufLock[i] = new ReentrantLock();
    }

    protected int getLocalSiteID() {
        GDD gdd = GDD.getInstance();
        HashSet<String> addrList = new HashSet<String>();
        int defaultPort = Integer.parseInt(Configuration.getInstance()
                .getOption("SiteConfiguration.default.dataport"));

        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface
                    .getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                Enumeration<InetAddress> ips = interfaces.nextElement()
                        .getInetAddresses();
                while (ips.hasMoreElements()) {
                    addrList.add(ips.nextElement().getHostAddress());
                }
            }
            Vector<SiteMeta> sites = gdd.getSiteInfo();
            for (int i = 0; i < sites.size(); i++) {
                SiteMeta site = sites.get(i);
                if (addrList.contains(site.getSiteIP())
                        && site.getSitePort() == defaultPort) {
                    return site.getSiteID();
                }
            }
        } catch (SocketException ex) {
            System.out.println("SelectTableHandler:SelectTableHandler: "
                    + ex.toString());
        }
        return -1;
    }
    
    public long getTransferSize() {
        return transferSize;
    }

    public String[] createTempTable(String sql, String t, boolean keyable)
            throws SQLException {
        DbColumn[] cols = dbm.getSelectResultColumns(sql);
        String[] types = new String[cols.length];
        String[] resultAttrList = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            String [] colSplit = cols[i].column.split("_", 2);
            ColumnInfo info = GDD.getInstance().getColumnInfo(colSplit[0], colSplit[1]);
            resultAttrList[i] = cols[i].column;
            types[i] = info.getColumnTypeName();
            if (keyable && info.getColumnKeyable() == 1)
                types[i] += " unique key ";
        }
        dbm.createTempTable(t, resultAttrList, types);
        return resultAttrList;
    }
    
    public DbTable sendExecutionPlan(TreeNode root) {
        DbTable table = null;
        /*if (root.getSiteID() == this.siteId) {
            LocalSelectExecutePlan localExe = new LocalSelectExecutePlan(dbm, null, tableBuffer, root.getNodeID(), bufLock);
            transferSize += localExe.getTransferSize();
            String tableName = localExe.executeLocal(root);
            try {
                table = dbm.executeSelect("select * from " + tableName);
                return table;
            } catch (SQLException ex) {
                System.out.println("DataServer : sendExecutionPlan "+ex.toString());
            }
        }*/
        SiteMeta site = GDD.getInstance().getSiteInfo(root.getSiteID());
        SelectExecuteResult exeResult = new SelectExecuteResult(root, classesCount, uid);
        ClientBase client = new ClientBase(site.getSiteIP(), site.getSitePort());
        long start = System.currentTimeMillis();
        System.out.println("start sending...to " + site.getSiteID());
        try {
            SiteExecuteResult result = (SiteExecuteResult)client.sendContext("selecttable", exeResult);
            table = result.getTable();
            transferSize += (result.getTransSize() + calculateTransferSize(table)) / 1024;
            
        } catch (IOException ex) {
            System.out.println("DataServer: sendExecutionPlan " + ex.toString());
        }
        long end = System.currentTimeMillis();
        System.out.println("Sending execution plan to site : " + site.getSiteID() + "  " + (end - start) + " " + transferSize + "K transferred");
        return table;
    }
    
    public long calculateTransferSize(DbTable table) {
        long size = 0, unit = 0;
        ArrayList<String> colList = table.getColName();
        for (int i = 0; i < colList.size(); i++) {
            String [] colSplit = colList.get(i).split("_", 2);
            ColumnInfo info = GDD.getInstance().getColumnInfo(
                    colSplit[0], colSplit[1]);
            int type = info.getColumnType();
            switch (type) {
            case CONSTANT.VALUE_INT:
                unit += 4;
                break;
            case CONSTANT.VALUE_STRING:
                unit += info.getColumnLength()/2;
                break;
            case CONSTANT.VALUE_DOUBLE:
                unit += 8;
                break;
            default:
                break;
            }
        }
        size = unit * table.getRowCount();
        return size;
    }

    protected String expandAttribute(String attr) {
        String[] col = attr.split("\\.", 2);
        String str = "";
        if (col[1].equals("*")) {
            TableMeta tinfo = GDD.getInstance().getTableInfo(col[0]);
            Iterator<ColumnInfo> it = tinfo.getColumnInfo().iterator();
            while (it.hasNext()) {
                str += col[0] + "_" + it.next().getColumnName();
                if (it.hasNext()) {
                    str += ",";
                }
            }
        }
        else
            str = col[0] + "_" + col[1];
        return str;
    }
    
    public String newSelectQuery(String tableName) {
        String sql = "select ";
        if (attrNameList.size() == 0) {
            sql += " * ";
        } else {
            for (int i = 0; i < attrNameList.size() - 1; i++) {
                sql += expandAttribute(attrNameList.get(i)) + ",";
            }
            sql += expandAttribute(attrNameList.get(attrNameList.size() - 1));
        }
        sql += " from " + tableName + " ";
        if (exprList.size() > 0) {
            sql += "where ";
            for (int i = 0; i < exprList.size() - 1; i++) {
                SimpleExpression expr = exprList.get(i);
                sql += expr.tableName + "_" + expr.columnName + expr.op
                        + expr.value + " and ";
            }
            SimpleExpression expr = exprList.get(exprList.size() - 1);
            sql += expr.tableName + "_" + expr.columnName + expr.op
                    + expr.value;
        }
        return sql;
    }

    public String newSingleJoinQuery(String leftTable, String rightTable,
            String joinCol) {
        String sql = "select ";
        if (attrNameList.size() == 0) {
            sql += " * ";
        } else {
            for (int i = 0; i < attrNameList.size() - 1; i++) {
                sql += expandAttribute(attrNameList.get(i)) + ",";
            }
            sql += expandAttribute(attrNameList.get(attrNameList.size() - 1));
        }
        sql += " from " + leftTable + " join " + rightTable + " ";
        sql += "using (" + joinCol + ")";
        sql += getExprString('_');
        return sql;
    }

    public String newJoinQuery(String leftTable, String rightTable,
            ArrayList<SimpleExpression> joinExprList) {
        String sql = "select ";
        if (attrNameList.size() == 0) {
            sql += " * ";
        } else {
            for (int i = 0; i < attrNameList.size() - 1; i++) {
                sql += expandAttribute(attrNameList.get(i)) + ",";
            }
            sql += expandAttribute(attrNameList.get(attrNameList.size() - 1));
        }
        sql += " from " + leftTable + "," + rightTable + " ";
        sql += getExprString('_');
        sql += exprList.size() == 0 ? " where " : " and ";
        Iterator<SimpleExpression> it = joinExprList.iterator();
        while (it.hasNext()) {
            SimpleExpression joinExpr = it.next();
            sql += leftTable + "." + joinExpr.tableName + "_"
                    + joinExpr.columnName + joinExpr.op + rightTable + "."
                    + joinExpr.value;
            if (it.hasNext())
                sql += " and ";
        }
        return sql;
    }

    public String getExprString(char delimiter) {
        String exprStr = "";

        if (exprList.size() > 0) {
            exprStr += "where ";
            for (int i = 0; i < exprList.size() - 1; i++) {
                SimpleExpression expr = exprList.get(i);
                exprStr += expr.tableName
                        + delimiter
                        + expr.columnName
                        + expr.op
                        + (expr.valueType == CONSTANT.VALUE_STRING ? "'"
                                + expr.value + "'" : expr.value) + " and ";
            }
            SimpleExpression expr = exprList.get(exprList.size() - 1);
            exprStr += expr.tableName
                    + delimiter
                    + expr.columnName
                    + expr.op
                    + (expr.valueType == CONSTANT.VALUE_STRING ? "'"
                            + expr.value + "'" : expr.value);
        }

        return exprStr;
    }

    public DbTable execute(TreeNode root) {
        DbTable table = null;
        
        if (root.getSiteID() != siteId) {
            sendExecutionPlan(root);
        }

        if (root.isLeaf()) {
            LeafNode leaf = (LeafNode) root;
            //GDD gdd = GDD.getInstance();
            
            String sql = "select ";
            if (attrNameList.size() == 0) {
                sql += " * ";
            } else {
                for (int i = 0; i < attrNameList.size() - 1; i++) {
                    sql += attrNameList.get(i) + ",";
                }
                sql += attrNameList.get(attrNameList.size() - 1) + " ";
            }
            sql += "from " + leaf.getTableName() + " ";
            sql += getExprString('.');

            System.out.println(sql);

            try {
                table = dbm.executeSelect(sql);
                ArrayList<String> tableList = table.getTableName();
                ArrayList<String> colList = table.getColName();
                for (int i = 0; i < tableList.size(); i++) {
                    String tableName = GDD.getInstance()
                            .getTableNameofFragmentation(tableList.get(i));
                    ColumnInfo info = GDD.getInstance().getColumnInfo(
                            tableName, colList.get(i));
                    if (info.getColumnKeyable() == 1)
                        table.setKey(i);
                    colList.set(i, tableName + "_" + colList.get(i));
                }
            } catch (Exception ex) {
                System.out.println(ex.toString());
            }
        }

        if (root instanceof ProjectionNode) {
            ArrayList<String> tableList = ((ProjectionNode) root)
                    .getTableNameList();
            ArrayList<String> attrList = ((ProjectionNode) root)
                    .getAttributeList();
            for (int i = 0; i < attrList.size(); i++) {
                String tableName = tableList.get(i).equals("null") ? ""
                        : tableList.get(i) + ".";
                attrNameList.add(tableName + attrList.get(i));
            }
            return execute(root.getChild(0));
        }

        if (root instanceof SelectionNode) {
            List<SimpleExpression> simpExprList = ((SelectionNode) root)
                    .getCondList();
            for (int i = 0; i < simpExprList.size(); i++) {
                exprList.add(simpExprList.get(i));
            }
            return execute(root.getChild(0));
        }

        if (root instanceof UnionNode) {
        	long start = System.currentTimeMillis();
            UnionNode union = (UnionNode) root;
            ArrayList<TreeNode> childList = union.getChildList();
            String tempTable = getTempName(union.getNodeID());
            String [] unionTables = new String[childList.size()];
            CountDownLatch barrier = new CountDownLatch(childList.size());
            for (int i = 0; i < childList.size(); i++) {
                new Thread(new JoinTask(childList.get(i), barrier, unionTables, i, null)).start();
            }
            try {
                barrier.await();
            } catch (InterruptedException ex) {
                System.out.println("SelectExecutionPlan : " + ex.toString());
            }
            String[] attrList = null;
            //attrList = this.createTempTable("select * from " + unionTables[0], tempTable, false);
            for (int i = 0; i < unionTables.length; i ++) {
                try {
                    if (i == 0) {
                        dbm.createTableLike(tempTable, unionTables[0]);
                    }
                    DbColumn [] cols;
                    cols = dbm.getSelectResultColumns("select * from " + unionTables[i]);
                    attrList = new String[cols.length];
                    for (int j = 0; j < cols.length; j ++)
                        attrList[j] = cols[j].column;

                    dbm.selectIntoTable(tempTable, attrList,
                            "select * from " + unionTables[i]);
                } catch (SQLException ex) {
                    System.out.println("DataServer: Union "
                            + ex.toString());
                }
            }
            
            String sql = newSelectQuery(tempTable);
            System.out.println(sql);
            try {
                table = dbm.executeSelect(sql);
                dbm.dropTable(tempTable);
            } catch (SQLException ex) {
                System.out.println("DataServer: Union select " + ex.toString());
            }
            long end = System.currentTimeMillis();
            System.out.println("Union time : " + (end - start));
        }

        if (root instanceof JoinNode) {
            JoinNode join = (JoinNode) root;
            HashMap<String, String> joinAttr = join.getAttributeList();
            Iterator<String> it = joinAttr.keySet().iterator();
            boolean single = false;
            String joinCol = null;
            if (joinAttr.keySet().size() == 1)
                single = true;
            ArrayList<SimpleExpression> joinExprList = new ArrayList<SimpleExpression>();
            while (it.hasNext()) {
                String col = it.next();
                String left = GDD.getInstance().getTableNameofFragmentation(
                        join.getLeftTableName());
                if (left == null) {
                    left = join.getLeftTableName();
                }
                String right = GDD.getInstance().getTableNameofFragmentation(
                        join.getRightTableName());
                if (right == null) {
                    right = join.getRightTableName();
                }
                if (single && left.equals(right)
                        && col.equals(joinAttr.get(col))) {
                    joinCol = left + "_" + col;
                }
                SimpleExpression joinExpr = new SimpleExpression(left, col,
                        "=", right + "_" + joinAttr.get(col), 0);
                joinExprList.add(joinExpr);
            }

            ArrayList<TreeNode> childList = join.getChildList();
            String leftTable = null, rightTable = null;
            CountDownLatch barrier = new CountDownLatch(2);
            String [] joinTables = new String[2];
            for (int i = 0; i < childList.size(); i++) {
                new Thread(new JoinTask(childList.get(i), barrier, joinTables, i, joinCol)).start();
            }
            try {
                barrier.await();
            } catch (InterruptedException ex) {
                System.out.println("SelectExecutionPlan : " + ex.toString());
            }
            leftTable = joinTables[0];
            rightTable = joinTables[1];

            String sql;
            if (single && joinCol != null)
                sql = newSingleJoinQuery(leftTable, rightTable, joinCol);
            else
                sql = newJoinQuery(leftTable, rightTable, joinExprList);
            System.out.println(sql);

            try {
                if (dbm.executeSelect("show keys from " + leftTable).getRowCount() == 0 &&
                        dbm.executeSelect("show keys from " + rightTable).getRowCount() == 0) {
                    Iterator<SimpleExpression> eit = joinExprList.iterator();
                    while (eit.hasNext()) {
                        dbm.createIndex(rightTable, eit.next().value);
                    }
                }
                long start = System.currentTimeMillis();
                table = dbm.executeSelect(sql);
                if (single && joinCol != null) {
                    ArrayList<String> colList = table.getColName();
                    for (int i = 0; i < colList.size(); i++) {
                        String[] colInfo = colList.get(i).split("_", 2);
                        ColumnInfo info = GDD.getInstance().getColumnInfo(
                                colInfo[0], colInfo[1]);
                        if (info.getColumnKeyable() == 1) {
                            table.setKey(i);
                            System.out.println("Vertical fragment : enable key");
                        }
                    }
                }
                long end = System.currentTimeMillis();
                System.out.println("Selection Join : " + (end - start) + "ms");
            } catch (SQLException ex) {
                System.out.println("DataServer: Join select " + ex.toString());
            }
        }

        return table;
    }
}
