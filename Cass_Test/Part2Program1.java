package Cass_Test;
import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.PreparedStatement;


public class Part2Program1 {

	public static void main(String[] args)
	{
		Session session;
		Cluster cluster;
		/*Retry policy is helpful when there are no failure or unavailabilities due to read timeout or write timeout*/
		/* Client can request on any node .This might overload a particular node making the node inefficient.Load Balancing policy is 
		 * used to distribute the load across various nodes*/
		/*Token Aware policy is ensure that request would go to the node or replica responsible to that data indicated by the primary key*/
		
		cluster = Cluster
				.builder()
				.addContactPoint("127.0.0.1")
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withLoadBalancingPolicy(
		                         new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))//DCAwareRoundRobinPolicy.builder().build() is a recommended method
				.build();
			session = cluster.connect("ecommerce");
			
		/* Use of Prepared Statement is more advisable as it would parse the query only once and bound the values to the variable
		 * which are then executed*/
			
		 PreparedStatement statement=session.prepare("insert into products" + "(pdt_id, cat_id, pdt_name, pdt_desc, price, shipping)" + "values(?,?,?,?,?,?);");
		 
		 BoundStatement boundStatement=new BoundStatement(statement);
		 session.execute(boundStatement.bind(0, 106, "Hair drier",
					"No risk to extreme heat", 2.9f,"Arriving"));
		 //Use of query builder to extract data from cassandra DB.Query builder is more secure
		 
		 Statement selectStatement=QueryBuilder.select().all().from("ecommerce","products").where(QueryBuilder.eq("pdt_id", 0));
		 ResultSet resultSet=session.execute(selectStatement);
		 for(Row row : resultSet)
		 {
			 System.out.println("Product ID:"+Integer.toString(row.getInt("pdt_id")));
			 System.out.println("Product Name:"+row.getString("pdt_name"));
			 System.out.println("Product Description:"+row.getString("pdt_desc"));
			 System.out.println("Price:"+row.getFloat("price"));	
		 }
		 cluster.close();
		 }
		  
	}
	
