package Cass_Test;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
public class Part2DeleteQuery {
  public static void main(String[] args)
  {
	  Cluster cluster;
		Session session;
		cluster=Cluster.builder().addContactPoint("127.0.0.1").withRetryPolicy(DefaultRetryPolicy.INSTANCE).
				withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy())).build();
		session=cluster.connect("ecommerce");
		/*Update*/
		Statement updateStatement=QueryBuilder.delete().from("ecommerce","products").where(QueryBuilder.eq("pdt_id", 5));
		session.execute(updateStatement);
		Statement selectStatement=QueryBuilder.select().all().from("ecommerce","products");
		ResultSet resultSet=session.execute(selectStatement);
		if(resultSet!=null)
		{
			for(Row row:resultSet)
			{
				 System.out.println("Product ID:"+Integer.toString(row.getInt("pdt_id")));
				 System.out.println("Product Name:"+row.getString("pdt_name"));
				 System.out.println("Product Description:"+row.getString("pdt_desc"));
				 System.out.println("Price:"+row.getFloat("price"));	
			}
			}
		cluster.close();
  }
  
}
