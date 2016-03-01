package sqlParser;
import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.Union;


public class SelectItemsFinder implements SelectVisitor{
	private ArrayList<String> selectItemsList;
	public ArrayList<String> getSelectItemsList(Select select){
		selectItemsList = new ArrayList<String>();
		select.getSelectBody().accept(this);

		return selectItemsList;
	}
	@Override
	public void visit(PlainSelect plainSelect) {
		
		//selectItemsList.addAll((ArrayList<String>) plainSelect.getSelectItems());
		List<?> list = plainSelect.getSelectItems();
		for(int i=0;i<list.size();++i){
			String ans = "" + list.get(i);
			selectItemsList.add(ans);
		}
		//String aa = PlainSelect.getStringList(plainSelect.getSelectItems());
		//System.out.println(aa);
		//selectItemsList.add("aa");
		
	}

	@Override
	public void visit(Union union) {
	}

}
