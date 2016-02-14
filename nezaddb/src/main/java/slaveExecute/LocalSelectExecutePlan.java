package slaveExecute;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;

import dbcore.DbColumn;
import dbcore.DbManager;
import gdd.ColumnInfo;
import gdd.GDD;
import globalDefinition.SimpleExpression;
import queryTree.JoinNode;
import queryTree.LeafNode;
import queryTree.ProjectionNode;
import queryTree.SelectionNode;
import queryTree.TreeNode;
import queryTree.UnionNode;

public class LocalSelectExecutePlan extends SelectExecutePlan {

    //private DbManager dbm = null;
    private String tempTable = null;
    private GDD gdd = null;
    private int node;

    public LocalSelectExecutePlan() {
        siteId = getLocalSiteID();
        gdd = GDD.getInstance();
    }

    public LocalSelectExecutePlan(DbManager dbm, String table, String [] buf, int node, Lock [] bufLock, long uid) {
        siteId = getLocalSiteID();
        this.dbm = dbm;
        tempTable = table;
        this.tableBuffer = buf;
        this.node = node;
        this.bufLock = bufLock;
        this.classesCount = buf.length;
        this.uid = uid;
        gdd = GDD.getInstance();
    }

    public String executeLocal(TreeNode root) {
        //DbTable table = null;

        if (root.getSiteID() != siteId) {
            sendExecutionPlan(root);
        }

        if (root.isLeaf()) {
            LeafNode leaf = (LeafNode) root;

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
                DbColumn[] cols = dbm.getSelectResultColumns(sql);
                String[] types = new String[cols.length];
                String[] attrList = new String[cols.length];
                for (int i = 0; i < cols.length; i++) {
                    String tableName = gdd
                            .getTableNameofFragmentation(cols[i].table);
                    ColumnInfo info = gdd.getColumnInfo(tableName,
                            cols[i].column);
                    attrList[i] = tableName + "_" + cols[i].column;
                    types[i] = info.getColumnTypeName();
                    if (info.getColumnKeyable() == 1)
                        types[i] += " unique key ";
                }
                tempTable = getTempName(node);
                dbm.createTempTable(tempTable, attrList, types);
                dbm.selectIntoTable(tempTable, attrList, sql);
                return tempTable;
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
            return executeLocal(root.getChild(0));
        }

        if (root instanceof SelectionNode) {
            List<SimpleExpression> simpExprList = ((SelectionNode) root)
                    .getCondList();
            for (int i = 0; i < simpExprList.size(); i++) {
                exprList.add(simpExprList.get(i));
            }
            return executeLocal(root.getChild(0));
        }

        if (root instanceof UnionNode) {
            UnionNode union = (UnionNode) root;
            tempTable = getTempName(union.getNodeID());
            ArrayList<TreeNode> childList = union.getChildList();
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
                    System.out.println("DataServer: Local Union "
                            + ex.toString());
                }
            }

            // If no more selection needed , return the table name immediately
            if (union.getNodeID() == node)
                return tempTable;

            String sql = newSelectQuery(tempTable);
            System.out.println(sql);
            try {
                tempTable = getTempName(node);
                String[] resultAttrList = this.createTempTable(sql, tempTable,
                        false);
                // dbm.createTempTable(tempTable, resultAttrList, types);
                dbm.selectIntoTable(tempTable, resultAttrList, sql);
                return tempTable;
                // table = dbm.executeSelect(sql);
            } catch (SQLException ex) {
                System.out.println("DataServer: Local Union select "
                        + ex.toString());
            }
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
                System.out.println("DataServer: Local Join " + ex.toString());
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
                boolean keyable;
                if (single && joinCol != null)
                    keyable = true;
                else
                    keyable = false;
                tempTable = getTempName(node);
                String[] resultAttrList = this.createTempTable(sql, tempTable,
                        keyable);
                long start = System.currentTimeMillis();
                if (dbm.executeSelect("show keys from " + leftTable).getRowCount() == 0 &&
                        dbm.executeSelect("show keys from " + rightTable).getRowCount() == 0) {
                    Iterator<SimpleExpression> eit = joinExprList.iterator();
                    while (eit.hasNext()) {
                        dbm.createIndex(rightTable, eit.next().value);
                    }
                }
                dbm.selectIntoTable(tempTable, resultAttrList, sql);
                long end = System.currentTimeMillis();
                System.out.println("Selection Join : " + (end - start) + "ms");
                return tempTable;
            } catch (SQLException ex) {
                System.out.println("DataServer: Join select " + ex.toString());
            }
        }

        return "";
    }
}
