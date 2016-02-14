package execute;

import java.util.Vector;

import executeReturnResult.GDDReturnResult;
import gdd.ColumnInfo;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.SiteInfo;
import gdd.TableInfo;
import globalDefinition.CONSTANT;

public class GDDExecute {
	private GDDReturnResult gddReturnResult;

	public GDDExecute() {
		this.gddReturnResult = new GDDReturnResult();
	}

	public GDDReturnResult getResult() {
		return this.gddReturnResult;
	}

	public void genGDDInfos() {
		GDD gdd = GDD.getInstance();
		gdd.GDDReader(GDD.CONTROL_SERVER_CONFIG);

		gdd.printGDD();

		String s;
		this.gddReturnResult.tableinfoName = new Vector();
		this.gddReturnResult.tableinfoName.add("TableName");
		this.gddReturnResult.tableinfoName.add("ColumnName");
		this.gddReturnResult.tableinfoName.add("FragType");
		this.gddReturnResult.tableinfoName.add("FragName");
		this.gddReturnResult.tableinfoName.add("FragSize");

		Vector<TableInfo> tableInfos = gdd.getTableInfos();
		for (int i = 0; i < tableInfos.size(); i++) {
			Vector<String> singletable = new Vector();
			TableInfo tableinfo = tableInfos.elementAt(i);
			// s = "tableName="+ tableinfo.getTableName();
			// s +=",  columnSize="+tableinfo.getColNum();
			// /this.gddTableInfos.add(s);
			// this.gddReturnResult.addTableinfo(s);
			singletable.add(tableinfo.getTableName());
			Vector<ColumnInfo> colinfos = tableinfo.getColumnInfo();
			s = "";
			for (int j = 0; j < colinfos.size(); j++) {
				ColumnInfo colinfo = colinfos.elementAt(j);
				s += colinfo.getColumnName();
				if (j < colinfos.size() - 1)
					s += ",";
			}
			singletable.add(s);

			if (tableinfo.getFragType() == CONSTANT.FRAG_HORIZONTAL) {
				singletable.add("Horizontal");
			} else if (tableinfo.getFragType() == CONSTANT.FRAG_VERTICAL) {
				singletable.add("Vertical");
			} else
				singletable.add("");

			String name = "";
			String size = "";
			Vector<FragmentationInfo> fragInfos = tableinfo
					.getFragmentationInfo();
			for (int j = 0; j < fragInfos.size(); j++) {
				FragmentationInfo fraginfo = fragInfos.elementAt(j);
				// s =
				// "fragName="+fraginfo.getFragName()+",  fragCondition="+fraginfo.getFragCondition()
				// +",  fragSize="+fraginfo.getFragSize();
				// this.gddTableInfos.add(s);
				// this.gddReturnResult.addTableinfo(s);
				name += fraginfo.getFragName();
				size += fraginfo.getFragSize();
				if (j < fragInfos.size() - 1) {
					name += ",";
					size += ",";
				}
			}
			singletable.add(name);
			singletable.add(size);
			// this.gddTableInfos.add("");
			this.gddReturnResult.gddTableInfos.add(singletable);
		}

		Vector<SiteInfo> siteinfos = gdd.getSiteInfo();
		this.gddReturnResult.siteInfoName = new Vector();
		this.gddReturnResult.siteInfoName.add("siteName");
		this.gddReturnResult.siteInfoName.add("IP");
		this.gddReturnResult.siteInfoName.add("PORT");
		this.gddReturnResult.siteInfoName.add("SiteFragemtation");
		for (int i = 0; i < siteinfos.size(); i++) {
			Vector<String> singlesite = new Vector<String>();
			SiteInfo siteinfo = siteinfos.elementAt(i);
			singlesite.add(siteinfo.getSiteName());

			singlesite.add(siteinfo.getSiteIP());
			singlesite.add("" + siteinfo.getSitePort());
			s = "";
			for (int j = 0; j < siteinfo.getSiteFragNames().size(); j++) {
				s += siteinfo.getSiteFragNames().elementAt(j);
				if (j < siteinfo.getSiteFragNames().size() - 1)
					s += ",";
			}
			singlesite.add(s);
			this.gddReturnResult.gddSiteInfos.add(singlesite);
		}

	}
}
