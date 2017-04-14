package Cass_Test;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class DeleteQuery {
	public static void main(String[] args){
	Cluster cluster;
	Session session;
	
	cluster=Cluster.builder().addContactPoint("localhost").build() ;
	session=cluster.connect("ecommerce");
	session.execute("Delete from products where pdt_id=3");
	String pid=null,pname=null,pdesc=null;
	float price=0;
	ResultSet resultset=session.execute("select * from products");
	for(Row row:resultset)
	{
		pid=Integer.toString(row.getInt("pdt_id"));
		pname=row.getString("pdt_name");
		pdesc=row.getString("pdt_desc");
		price=row.getFloat("price");
		System.out.println("Product ID:"+pid);
		System.out.println("Product Name:"+pname);
		System.out.println("Product Description:"+ pdesc);
		System.out.println("Price:"+price);
	}
	cluster.close();
}
}


