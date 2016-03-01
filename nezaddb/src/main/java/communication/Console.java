package communication;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import queryTree.FormattedTreeNode;

import configuration.Configuration;
import executeReturnResult.ErrorReturnResult;
import executeReturnResult.ImportReturnResult;
import executeReturnResult.ParseTreeReturnResult;
import executeReturnResult.SelectReturnResult;
import executeReturnResult.TestSiteExecuteReturn;

public class Console {
    public static String input() {
        BufferedReader bufReader = new BufferedReader(new InputStreamReader(
                System.in));
        String cmd = "exit";
        try {
            cmd = bufReader.readLine();
        } catch (IOException ex) {
            cmd = "exit";
        }
        return cmd;
    }

    public static void main(String args[]) {
        String cmd = null;
        boolean optimization = true;
        boolean parseOnly = false;
        int defaultPort = Integer.parseInt(Configuration.getInstance()
                .getOption("SiteConfiguration.default.ctrlport"));
        String addr = Configuration.getInstance().getOption(
                "SiteConfiguration.controlsite.addr");
        ClientBase client = new ClientBase(addr, defaultPort);
        while (!(cmd = input()).equals("exit")) {
            try {
                if (cmd.length() > 11
                        && cmd.substring(0, 11).equals("import data")) {
                    String fileList = cmd.substring(12);
                    StringTokenizer commaToker = new StringTokenizer(fileList,
                            ",");
                    String path;
                    ArrayList<String> pathList = new ArrayList<String>();  
                    while (commaToker.hasMoreTokens()) {
                        path = commaToker.nextToken();
                        File transFile = new File(path);
                        pathList.add(transFile.getName());
                        System.out.println("send file");
                        String res = client.sendFile("executecmd", transFile, transFile.getName(),
                                "image/jpeg");
                        System.out.println(res);
                    }
                    cmd = "import data ";
                    for (int i = 0; i < pathList.size() - 1; i ++) {
                        cmd += pathList.get(i);
                        cmd += ",";
                    }
                    cmd += pathList.get(pathList.size() - 1);
                    System.out.println(cmd);
                    //continue;
                }
                
                if (cmd.length() > 4 && cmd.substring(0, 4).equals("init")) {
                    String path = cmd.substring(5);
                    File transFile = new File(path);
                    System.out.println("send file");
                    String res = client.sendFile("executecmd", transFile, transFile.getName(),"image/jpeg");
                    System.out.println(res);
                    cmd = "init " + transFile.getName();
                }
                
                if (cmd.equals("testsite")) {
                    TestSiteExecuteReturn rs = (TestSiteExecuteReturn)client.sendContext("testsite", "");
                    System.out.println(rs.getSiteStatus());
                    continue;
                }
                
                if (cmd.equals("enable opt")) {
                    optimization = true;
                    continue;
                }
                else if (cmd.equals("disable opt")) {
                    optimization = false;
                    continue;
                }
                
                if (cmd.equals("parse only")) {
                    parseOnly = true;
                    continue;
                }
                else if (cmd.equals("normal exec")) {
                    parseOnly = false;
                    continue;
                }
                
                if (cmd.equals("reset")) {
                    String r = (String)client.sendContext("reset", "");
                    if (r != null) {
                        System.out.println("reset from server : " + r);
                    }
                    continue;
                }
                
                HashMap<String, String> options = new HashMap<String, String>();
                if (optimization == false) {
                    options.put("optimization", "disable");
                }
                if (parseOnly) {
                    options.put("execution", "parse only");
                }
                
                Object obj = client.sendContext("executecmd", cmd, options);
                if (obj instanceof String) {
                    System.out.println((String) obj);
                }
                if (obj instanceof ImportReturnResult) {
                    ImportReturnResult rs = (ImportReturnResult)obj;
                    rs.displayResult();
                }
                if (obj instanceof ErrorReturnResult) {
                    ErrorReturnResult err = (ErrorReturnResult)obj;
                    System.out.println(err.errorMsg());
                }
                if (obj instanceof SelectReturnResult) {
                    SelectReturnResult rs = (SelectReturnResult)obj;
                    System.out.println("Total Rows: " + rs.rows.size() + " @" + rs.totalResponseTime + "ms " + " size " + rs.totalTransferSize);
                }
                if (obj instanceof ParseTreeReturnResult) {
                    ParseTreeReturnResult rs = (ParseTreeReturnResult)obj;
                    List<FormattedTreeNode> list = rs.getTreeList();
                    Iterator<FormattedTreeNode> it = list.iterator();
                    
                    while (it.hasNext()) {
                        System.out.println(it.next().content);
                    }
                    System.out.println(list.size());
                }
            } catch (Exception ex) {
                System.out.println("Excetion Error , the site might be failed");
            }
        }
    }
}
