package execute;

import java.util.List;
import java.util.Vector;

import gdd.FragmentationCondition;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.TableMeta;
import globalDefinition.CONSTANT;
import parserResult.ParseResult;
import parserResult.VFragmentResult;

public class VFragmentExecute extends ExecuteSQL{
	
	private GDD gdd;
	
	public VFragmentExecute(){
		gdd = GDD.getInstance();
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		VFragmentResult fragmentResult = (VFragmentResult)result;
		String tableName = fragmentResult.getTableName();
		
		if(!gdd.isTableExist(tableName)){
			System.out.println(tableName + "is not exist! can't fragment the table!");
			System.exit(-1);
		}
		
		TableMeta tableInfo = gdd.getTableInfo(tableName);
		List subTable = fragmentResult.getSubTableList();
		List<List>columns = fragmentResult.getColumns();
		tableInfo.setFragmentationNum(subTable.size());
		tableInfo.setFragmentationType(CONSTANT.FRAG_VERTICAL);
		
		int i,j;
		String conditionString;
		for(i = 0 ; i < subTable.size() ; i++){
			List column = columns.get(i);
			conditionString = "(";
			for(j = 0 ; j < (column.size()-1) ; j++)
				conditionString = conditionString + column.get(j).toString() + ":";
			conditionString = conditionString + column.get(column.size()-1).toString()+")";
			FragmentationInfo fraginfo = new FragmentationInfo(CONSTANT.FRAG_VERTICAL,subTable.get(i).toString(),conditionString,0,0);
			
			Vector<String> columnStrings = new Vector();
			for(j = 0 ; j < column.size() ; j++)
				columnStrings.add(column.get(j).toString());
			FragmentationCondition fragCondition = new FragmentationCondition(CONSTANT.FRAG_VERTICAL,columnStrings,null);
			fraginfo.setFragmentationCondition(fragCondition);
			tableInfo.getFragmentationInfo().add(fraginfo);	
		}
	}

}
