package executeReturnResult;

import java.util.Vector;

public class GDDReturnResult extends ExecuteReturnResult{
	
	
    static final long serialVersionUID = 1;
    public Vector<String> tableinfoName;
	public Vector<Vector<String>> gddTableInfos;
	public Vector<String> siteInfoName;
	public Vector<Vector<String>> gddSiteInfos;
	
	
	public GDDReturnResult(){
		this.gddTableInfos = new Vector<Vector<String>>();
		this.gddSiteInfos = new Vector<Vector<String>>();
		//this.genGDDInfos();
	}
	
	/*
	public void addTableinfo(String s){
		if(this.gddTableInfos == null)
			this.gddTableInfos = new Vector<String>();
		this.gddTableInfos.add(s);
	}
	*/
	/*
	public void addSiteinfo(String s){
		if(this.gddSiteInfos == null)
			this.gddSiteInfos = new Vector<>();
		this.gddSiteInfos.add(s);
	}
	*/
	/*
	public void genGDDInfos(){
		GDD gdd = GDD.getInstance();
		gdd.GDDReader(GDD.controlServerConfig);
		
		gdd.printGDD();
		
		String s;
		Vector<TableInfo> tableInfos = gdd.getTableInfos();
		for(int i = 0 ; i < tableInfos.size() ; i++){
			TableInfo tableinfo = tableInfos.elementAt(i);
			s = "tableName="+ tableinfo.getTableName();
			s +=",  columnSize="+tableinfo.getColNum();
			this.gddTableInfos.add(s);
			Vector<ColumnInfo> colinfos = tableinfo.getColumnInfo();
			for(int j = 0 ; j  < colinfos.size(); j++){
				ColumnInfo colinfo = colinfos.elementAt(j);
				s = "columnName="+colinfo.getColumnName()+",  columnType="+CONSTANT.DATATYPE[colinfo.getColumnType()];
				this.gddTableInfos.add(s);                                                                          
			}
			Vector<FragmentationInfo> fragInfos = tableinfo.getFragmentationInfo();
			for(int j = 0 ; j < fragInfos.size(); j++){
				FragmentationInfo fraginfo = fragInfos.elementAt(j);
				s = "fragName="+fraginfo.getFragName()+",  fragCondition="+fraginfo.getFragCondition()
				    +",  fragSize="+fraginfo.getFragSize();
				this.gddTableInfos.add(s);
			}
			this.gddTableInfos.add("");
		}
		
		
		Vector<SiteInfo> siteinfos = gdd.getSiteInfo();
		for(int i = 0 ; i < siteinfos.size(); i++){
			SiteInfo siteinfo = siteinfos.elementAt(i);
			s = "siteName="+siteinfo.getSiteName();
			this.gddSiteInfos.add(s);
			this.gddSiteInfos.add("IP: "+siteinfo.getSiteIP());
			this.gddSiteInfos.add("PORT: "+siteinfo.getSitePort());
			s = "siteFragmentation: ";
			for(int j = 0 ; j < siteinfo.getSiteFragNames().size(); j++)
			{
				s += siteinfo.getSiteFragNames().elementAt(j);
				if(j < siteinfo.getSiteFragNames().size() - 1)
					s += ",";
			}
			this.gddSiteInfos.add(s);
			this.gddSiteInfos.add("");
		}
		
		
		
	}
	*/
	@Override
	public void displayResult() {
		// TODO Auto-generated method stub
		for(int i = 0 ; i < this.tableinfoName.size(); i++)
			System.out.print(" "+this.tableinfoName.elementAt(i));
		System.out.println();
		
		for(int i = 0 ; i < this.gddTableInfos.size(); i++){
			for(int j = 0 ; j < this.gddTableInfos.elementAt(i).size(); j++)
				System.out.print(" "+this.gddTableInfos.elementAt(i).elementAt(j));
			System.out.println();
		}
		System.out.println();
		
		for(int i = 0 ; i < this.siteInfoName.size(); i++)
			System.out.print(" "+this.siteInfoName.elementAt(i));
		System.out.println();
		
		for(int i = 0 ; i < this.gddSiteInfos.size(); i++){
			for(int j = 0 ; j < this.gddSiteInfos.elementAt(i).size(); j++)
				System.out.print(" "+this.gddSiteInfos.elementAt(i).elementAt(j));
			System.out.println();
		}
		
	}

}
