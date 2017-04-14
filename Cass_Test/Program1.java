package Cass_Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

public class Program1 {
	
	public static void main(String[] args)
	{
		Cluster cluster;
		Session session;
		//cluster connects to the address of the node provided.One contact point is required.Good to have multiple
		cluster=Cluster.builder().addContactPoint("localhost").build();
		session=cluster.connect("ecommerce");
		session.execute("INSERT INTO products (pdt_id, cat_id, pdt_name, pdt_desc, price, shipping) VALUES (002,105, 'Candy 0.9 cu. ft. Washing Machine', 'Capacity of 1 cu. ft.10 different power levels', 64.00, 'Expedited')");
		session.execute("INSERT INTO products (pdt_id, cat_id, pdt_name, pdt_desc, price, shipping) VALUES (003,106, 'Prestige 0.9 cu.cm. Pressure Cooker', 'Capacity: 18 qt.', 70.00, 'Dispatched from warehouse')");
		session.execute("INSERT INTO products (pdt_id, cat_id, pdt_name, pdt_desc, price, shipping) VALUES (004,107, 'Honeywell Air Conditioner', 'The Honeywell 15 Pt. White Indoor Portable Evaporative Air Cooler Puts Cool on the Move', 130, 'Delivered')");
		session.execute("INSERT INTO products (pdt_id, cat_id, pdt_name, pdt_desc, price, shipping) VALUES (005,108, 'Philips Blender', 'Measures 5.75  L x 17.05', 64.00, 'Order Placed')");
		session.execute("INSERT INTO products (pdt_id, cat_id, pdt_name, pdt_desc, price, shipping) VALUES (006,109, 'Bajaj Juicer', 'Measures 5.75  L x 17', 38, 'Order Shipped')");
		String pdtid = null, pdtname = null, pdtdesc = null;
		float price = 0;
		ResultSet resultSet=session.execute("select * from products");
		for(Row row:resultSet)
		{
			pdtid = Integer.toString(row.getInt("pdt_id"));
			pdtname = row.getString("pdt_name");
			pdtdesc = row.getString("pdt_desc");
			price = row.getFloat("price");
			System.out.println("Product ID: " + pdtid);
			System.out.println("Name: " + pdtname);
			System.out.println("Description: " + pdtdesc);
			System.out.println("Price: "+ price);
		}
		cluster.close();
		}
		
	}
	

