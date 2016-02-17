package parserResult;

public interface ParseResultVisitor {
	public void visit(AllocateResult allocateResult);
	public void visit(HFragmentResult hFragmentResult);
	public void visit(ParseErrorResult parserErrorResult);
	public void visit(SelectResult selectResult);
	public void visit(SetSiteResult setSiteResult);
	public void visit(VFragmentResult vFragmentResult);
	public void visit(ImportDataResult importDataResult);
	public void visit(CreateTableResult createTableResult);
	public void visit(InsertResult insertResult);
	public void visit(DeleteResult deleteResult);
	public void visit(CreateDatabaseResult createDatabaseResult);
	public void visit(UseDatabaseResult useDatabaseResult);
	public void visit(InitResult initResult);
}
