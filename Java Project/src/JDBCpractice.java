import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.*;
import java.util.*;

public class JDBCpractice {
//	public static void main(String[] args) throws ClassNotFoundException ,SQLException{
//		Class.forName("com.mysql.cj.jdbc.Driver");
//		Connection con = DriverManager.getConnection("jdbc:mysql://localhost:/practice","root","Chinnipavani@1");
//		System.out.println("Connection Successful");
//		
//	}
	private static Connection connection=null;
	private static Scanner scanner=new Scanner(System.in);
	public static void main(String[] args) {
		JDBCpractice prac=new JDBCpractice();
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			String dbUrl="jdbc:mysql://localhost:3306/practice";
			String username="root";
			String password="Chinnipavani@1";
			connection = DriverManager.getConnection(dbUrl,username,password);
			System.out.println("Connection Successful");
			System.out.println("1.Insert Record");
			System.out.println("2.select Record");
			System.out.println("3. Select all records");
			System.out.println("4.Update record");
			System.out.println("5. delete record");
			System.out.println("6.transaction");
			System.out.println("7.batch processing");
			int choice=Integer.parseInt(scanner.nextLine());
			switch(choice) {
			case 1:
				insertRecord();
				break;
			case 2:
				selectRecord();
				break;
			case 3:
				selectAllRecords();
				break;
			case 4:
				updateRecord();
				break;
			case 5:
				deleteRecord();
				break;
			case 6:
				transaction();
				break;
			case 7:
				batchProcessing();
				break;
			default:
				System.out.println("Give a valid choice");
			}
		}
		catch(Exception e) {
			System.out.println("Connection Unsuccessful" + e.getMessage());
			
		}
//		finally {
//        	//closing the connection
//        	try {
//        		if(connection!=null&& !connection.isClosed()) {
//        			connection.close();
//        			System.out.println("connection closed");
//        		}
//        	}catch(Exception e) {
//        		System.out.println("Error closing connection");
//        	}
//        }
	}
	private static void insertRecord() throws SQLException {
		System.out.println("Inside insertRecord");
		System.out.println("Enter address:");
		String address=scanner.nextLine();
		scanner.nextLine();
		System.out.println("Enter percentage:");
		double percentage=scanner.nextInt();
		scanner.nextLine();
		String sql="insert into student(address,percentage) values(?,?)";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, address);
		preparedStatement.setDouble(2, percentage);
		int rows=preparedStatement.executeUpdate();
		if (rows>0) {
				System.out.println("Row got inserted");
		}
	}
	public static void selectRecord() throws SQLException{
		System.out.println("Inside select");
		System.out.println("Enter roll no");
		int number=Integer.parseInt(scanner.nextLine());
		//String sql="select * from student where rollno=1";
		String sql="select * from student where rollno="+number;
		Statement statement=connection.createStatement();
		ResultSet result=statement.executeQuery(sql);
		if(result.next()) {
			int rollno=result.getInt("rollno");
			String address=result.getString("address");
			double percentage=result.getDouble("percentage");
			System.out.println("roll number "+rollno);
			System.out.println("address "+address);
			System.out.println("percentage "+percentage);
		}else {
			System.out.println("No records");
		}
	}
	public static void selectAllRecords() throws SQLException{
		CallableStatement callableStatement=connection.prepareCall("(call GET_ALL)");
		ResultSet result=callableStatement.executeQuery();
		while(result.next()) {
			System.out.println("roll number "+result.getInt("rollno"));
			System.out.println("address "+result.getString("address"));
			System.out.println("percentage "+result.getDouble("percentage"));
		}
		
	}
	public static void updateRecord() throws SQLException{
		System.out.println("Enter roll no: ");
		int rollnum=Integer.parseInt(scanner.nextLine());
		//String sql="select * from student where rollno=1";
		String sql="select * from student where rollno="+rollnum;
		Statement statement=connection.createStatement();
		ResultSet result=statement.executeQuery(sql);
		if(result.next()) {
			int rollno=result.getInt("rollno");
			String address=result.getString("address");
			double percentage=result.getDouble("percentage");
			System.out.println("roll number "+rollno);
			System.out.println("address "+address);
			System.out.println("percentage "+percentage);
			
			System.out.println("what to be updated");
			System.out.println("1.address ");
			System.out.println("2.perccentage ");
			int choice=Integer.parseInt(scanner.nextLine());
			//update student set address='pamur' where rollno=1;
			String sqlQuery ="update student set ";
			switch(choice) {
			case 1:
				System.out.println("address to be updated");
				//sqlQuery=sqlQuery+"address='pamur' where rollno="+rollno;
				System.out.println("Enter new address");
				String newAddress=scanner.nextLine();
				sqlQuery=sqlQuery+"address=? where rollno="+rollno;
				PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
				preparedStatement.setString(1, address);
				int rows=preparedStatement.executeUpdate();
				if (rows>0) {
						System.out.println("Records get updated");
				}
				break;
			case 2:
				System.out.println("percentage to be updated");
				System.out.println("Enter new percentage ");
				double newpercentage=Double.parseDouble(scanner.nextLine());
				sqlQuery=sqlQuery+"percentage=? where rollno="+rollno;
				PreparedStatement preparedStatement1 = connection.prepareStatement(sqlQuery);
				preparedStatement1.setDouble(1, newpercentage);
				int rows1=preparedStatement1.executeUpdate();
				if (rows1>0) {
						System.out.println("Records get updated");
				}
				break;
		}
	}
	}
	public static void deleteRecord() throws SQLException{
		//System.out.println("Inside the delete record");
		//String sql="delete from student where rollno=1";
		System.out.println("Enter rollno: ");
		int number=Integer.parseInt(scanner.nextLine());
		String sql="delete from student where rollno="+number;
		Statement statement=connection.createStatement();
		ResultSet result=statement.executeQuery(sql);
	}
	public static void transaction() throws SQLException{
		connection.setAutoCommit(false);
		String sql1="insert into student (address,percentage) values('chennai',85)";
		String sql2="insert into student (address,percentage) values('kerala',90)";
		PreparedStatement preparedStatement=connection.prepareStatement(sql1);
		//preparedStatement.executeUpdate();
		int row1=preparedStatement.executeUpdate();
		preparedStatement=connection.prepareStatement(sql2);
		//preparedStatement.executeUpdate();
		int row2=preparedStatement.executeUpdate();
		if(row1>0 && row2>0) {
			connection.commit();
			System.out.println("Rows got inserted");
		}else {
			connection.rollback();
		}	
	}
	public static void batchProcessing() throws SQLException{
		connection.setAutoCommit(false);
		String sql1="insert into student (address,percentage) values('ap',80)";
		String sql2="insert into student (address,percentage) values('karnataka',70)";
		String sql3="insert into student (address,percentage) values('bangalore',75)";
		String sql4="insert into student (address,percentage) values('assam',79)";
		String sql5="insert into student (address,percentage) values('omr',69)";
		String sql6="insert into student (address,percentage) values('kundrathur',93)";
		String sql7="update student set percentage=39 where address=kundrathur";
		Statement statement=connection.createStatement();
		statement.addBatch(sql1);
		statement.addBatch(sql2);
		statement.addBatch(sql3);
		statement.addBatch(sql4);
		statement.addBatch(sql5);
		statement.addBatch(sql6);
		int[] rows=statement.executeBatch();
		for(int i:rows) {
			if(i>0) {
				continue;
			}
			else {
				connection.rollback();
			}
		}	
	}
}