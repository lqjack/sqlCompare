package execute;

import java.util.List;
import java.util.Vector;

import gdd.FragmentationCondition;
import gdd.FragmentationInfo;
import gdd.GDD;
import gdd.TableInfo;
import globalDefinition.CONSTANT;
import globalDefinition.SimpleExpression;
import parserResult.HFragmentResult;
import parserResult.ParseResult;

public class HFragmentExecute extends ExecuteSQL {
	
	private GDD gdd;
	
	public HFragmentExecute(){
		gdd = GDD.getInstance();
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		HFragmentResult fragmentResult = (HFragmentResult)result;
		String tableName = fragmentResult.getTableName();
		
		if(!gdd.isTableExist(tableName)){
			System.out.println(tableName + "is not exist! can't fragment the table!");
			System.exit(-1);
		}
		
		TableInfo tableInfo = gdd.getTableInfo(tableName);
		List subTable = fragmentResult.getSubTableList();
		List<List> conditions = fragmentResult.getConditions();
		List<HFragmentResult.HFragmentUnit> expressions = fragmentResult.getHFragmentMap();
		tableInfo.setFragmentationNum(subTable.size());
		tableInfo.setFragmentationType(CONSTANT.FRAG_HORIZONTAL);
		int i;
		for(i = 0 ; i < subTable.size() ; i++){
			List condition = conditions.get(i);
			HFragmentResult.HFragmentUnit fragUnit = expressions.get(i);
			String cond = "";
			for(int j = 0; j < (condition.size()-1) ; j++)
				cond = cond + condition.get(j).toString() + " and ";
			cond = cond + condition.get(condition.size()-1).toString();
			
			FragmentationInfo fraginfo = new FragmentationInfo(CONSTANT.FRAG_HORIZONTAL,subTable.get(i).toString(),cond,0,0);
			
			Vector<SimpleExpression> express = new Vector<SimpleExpression>();
			List<SimpleExpression> simples = fragUnit.getExpression();
			for(int j = 0 ; j < simples.size() ; j++ ){
				express.add(new SimpleExpression(simples.get(j)));
			}
			FragmentationCondition fragCondition = new FragmentationCondition(CONSTANT.FRAG_HORIZONTAL,null,express);
			
			fraginfo.setFragmentationCondition(fragCondition);
			tableInfo.getFragmentationInfo().add(fraginfo);
		}
	}

}
