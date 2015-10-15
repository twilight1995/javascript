package conn;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import Ynu.Sei.cpLibrary.BASIC.cpIn;

public class Conn {
	
static Connection con; // 声明Connection对象
static PreparedStatement sql;
	
	public Connection getConnection() {// 建立返回值为Connection的方法
		try {// 加载数据库驱动类
			Class.forName("com.mysql.jdbc.Driver");
			System.out.println("数据库驱动加载成功");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		try {// 通过访问数据库的URL获取数据库连接对象
			con = DriverManager.getConnection("jdbc:mysql"
					+ "://localhost:3306/test", "root","123456");
			System.out.println("数据库连接成功");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return con; // 按方法要求返回一个Connection对象
	}
	
	public static void main(String[] args) { // 主方法
		Conn c = new Conn(); // 创建本类对象
		con=c.getConnection(); // 调用连接数据库方法
		
		long num=0;
		
		try {
			
			cpIn in =new cpIn("E:\\2011_mozilla_activity_level2");
			
			while(in.hasNextLine())
			{
			String str= in.readLine();
			sql = con.prepareStatement("insert into 2011_mozilla_activity_level2 values(?,?,?,?,?,?,?)");
			sql.setString(1, str.split(";")[0]);
			sql.setString(2, str.split(";")[1]);
			sql.setString(3, str.split(";")[2]);
			sql.setString(4, str.split(";")[3]);
			sql.setString(5, str.split(";")[4]);
			sql.setString(6, str.split(";")[5]);
			if (str.split(";").length==6)
				sql.setString(7, " ");
			else
			  sql.setString(7, str.split(";")[6]);
			sql.executeUpdate();
			System.out.println("数据插入成功。"+"---"+num++);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		
}
	}
}


