package util;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnection {

	public Connection getConnection() {
		
		Connection conn = null;

		try {
			String URL = "jdbc:mariadb://127.0.0.1:3306/test";
			//String URL = "jdbc:mysql://localhost:3306/newcomer?characterEncoding=UTF-8&serverTimezone=UTC&useSSL=false";
			String ID = "root";
			String Password = "1111";
			Class.forName("org.mariadb.jdbc.Driver");
			//Class.forName("com.mysql.cj.jdbc.Driver");
			conn = DriverManager.getConnection(URL, ID, Password);
			return conn;
		} catch(Exception e) {
			e.printStackTrace();
		}
		return null;
	}

}
