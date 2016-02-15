package net.sf.jsqlparser;
import java.sql.SQLException;

import org.junit.Test;

import execute.Execute;


/**
		String sql = "select a,b,c,d from t1,t2 where a=3 and b=4 ";
		sql = "delete from abcdefg  where b=3 and c=5 and d<>6 and e>=7";
		sql = "drop table t1";
		sql = "SELECT ENAME FROM PROJ, EMP, ASG WHERE AGS.ENO = EMP.ENO AND ASG.JNO = PROJ.JNO AND ENAME <>'J.Doe' AND PROJ.NAME='CAD/CAM' AND (DUR=12 OR DUR=24)";
		sql = "select Product.name from Product where stocks is not null";
		sql = "select Product.name from Product";
		sql = "select a,b,c,d from t1,t2 where a=3 and b=4";
		sql = "SELECT ENAME FROM PROJ, EMP, ASG WHERE ASG.ENO = EMP.ENO AND ASG.JNO = PROJ.JNO AND PROJ.NAME='CAD/CAM' AND PROJ.DUR=12";
		sql = "VERTICAL fragment ON Product set (id,name) as Product1, (id,producer_id,stocks) as Product2";
		sql = "horizontal fragment on Customer set (id<110000) as Customer1,(id>=110000 and id<112500) as Customer2,(id>=1125000 and id<115000) as Customer3";
		sql ="allocate (Customer1,Product1,Producer1) on Site1, (Customer2,Producer2) on Site2,(Product3,Producer3) on Site3";
		sql = "import data file1,file2,file3 as";
		sql = "setsite site1 on 10.1.1.1:1111,site2 on 10.1.1.2:2222";
		sql = "create table Book (id INT key, title CHAR(100), authors char(25), publisher_id int, copies int)";
		sql = "select * from Product where Product.ENAME='HH'";
		sql = "insert into Producer(id,name,location) values(200201,'TCL','SH')";
		sql = "create table Order (customer_id int,book_id int,quantity int)";
		sql = "createdb abcd";
		sql = "init initPar";
		sql = "usedb abcd";
		SqlParser v = new SqlParser(sql);
		CreateTableResult r = (CreateTableResult)v.getResult();
		r.displayResult();
		Execute execute = new Execute();
		execute.execute(sql);
		sql  =  "insert into Publisher(id,name,nation) values(200201,'TCL','USA')";
		sql = "insert into Customer(id,rank,name) values(200201,1234,'XIMI')";
		sql = "delete from Publisher where Publisher.id<1";
		sql = "delete from Customer where rank=10000";
		sql = "select Customer.name from Customer where Customer.rank=10000";
		sql = "select Book.name from Book where Book.rank=10000";
		sql = "select * from Customer";
		sql = "select Customer.id,Publisher.nation, Book.id from Customer,Publisher,Book where Customer.id=Publisher.id and Publisher.id=Book.id and Publisher.id=100000 and Customer.rank>20";
		sql ="select Customer.name from Customer where Customer.rank>1";
		sql = "select Customer.name,Book.title from Customer,Book where Customer.id = Book.id and Book.id>205000 and Customer.name='AS'";
		sql = "select * from Customer";
		sql = "select Publisher.name from Publisher";
		sql = "select * from Book where Book.id = 200018";
		sql ="select Book.title from Book where copies > 9999";
		sql = "select Book.title from Book where copies>5000";
		sql = "select customer_id, quantity from Orders where Orders.quantity<=7";
		sql = "select Book.title,Book.copies,Publisher.name,Publisher.nation from Book,Publisher where Book.publisher_id=Publisher.id and Publisher.nation='USA' and Book.copies>1000";
		sql = "select Customer.name,Orders.quantity from Customer,Orders where Customer.id = Orders.customer_id";
		sql = "select Customer.name,Customer.rank,Orders.quantity from Customer,Orders where Customer.id=Orders.customer_id and Customer.rank=1";
		sql = "select Customer.name,Orders.quantity,Book.title from Customer,Orders,Book where Customer.id=Orders.customer_id and Book.id=Orders.book_id and Customer.rank=1 and Book.copies>5000";
		sql = "select Customer.*,Book.*, Orders.*, Publisher.* from Customer,Orders,Book,Publisher where Customer.id=Orders.customer_id and Book.id=Orders.book_id and Publisher.id = Book.publisher_id";
		sql = "select Book.*,Publisher.* from Book,Publisher where  Publisher.id = Book.publisher_id";
		sql  = "select * from Book";
 *
 */
public class JSQLTest{
	
	static String driverClass = "";
	static String url = "";
	static String userName = "";
	static String password = "";
	
	public static void main(String[] args) throws SQLException, ReflectiveOperationException {
		String sql = "select a,b,c,d from t1,t2 where a=3 and b=4 ";
		Execute execute2 = new Execute(driverClass, url, userName, password);
		execute2.execute(sql, true);
	}
	
	@Test
	public void test() throws SQLException, ReflectiveOperationException {
		
	}
}