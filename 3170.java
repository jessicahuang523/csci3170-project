// testing
import java.io.*;
import java.util.*;
import java.sql.*;

class project
{
	
	// connection method to connect to database
	public static Connection connect()
	{
		String dbAddress = "jdbc:mysql://projgw.cse.cuhk.edu.hk:2633/group23";
		String dbUsername = "Group23";
		String dbPassword = "3170group23";
		
		Connection con = null ;
		try
		{	
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection(dbAddress, dbUsername, dbPassword);
			// test whether the connection is successful
			// System.out.println("connected");
		}catch (ClassNotFoundException e)
		{
			System.out.println("[ERROR]: Java MySQL DB Driver not found!!");
			System.out.println(e);
			System.exit(0);
		}catch (SQLException e)
		{
			System.out.println(e);
		}
		return con ;
	}
	
	// statement creation method
	private static Statement getStatement(Connection con)
	{
		try
		{
			Statement stmt = con.createStatement();
			return stmt ;
		}catch (SQLException e)
		{ System.out.println("[ERROR] statement creation error." );
		  System.out.println(e.getMessage());
		  System.exit(0); 
		}catch (NullPointerException e)
		{ System.out.println("[ERROR] connection maybe not be properly setup.");
		}catch (Exception e)
		{ System.out.println("[ERROR] unknow error" + e); }
	
		// incase of unsuccessful connection, return null
		return null;
	}
	
	// PreparedStatement creation method
	private static PreparedStatement getPreparedStatement(Connection con, String line)
	{
		try
		{
			PreparedStatement pstmt = con.prepareStatement(line);
			return pstmt ;
		}catch (SQLException e)
		{ System.out.println("[ERROR] prepared statement creation error." );
		  System.out.println(e.getMessage());
		  System.exit(0); 
		}catch (NullPointerException e)
		{ System.out.println("[ERROR] connection maybe not be properly setup.");
		}catch (Exception e)
		{ System.out.println("[ERROR] unknow error" + e); }
		
		// incase of unsuccessful connection, return null
		return null;
	}
	
	private static void create_table(Statement stmt, String name, String content )
	{
		// Assumption : if one table exist, than all table exist, 
		
		// if a table does not exist, create it
		// if a table already exist, drop them, than create a new one
		try
		{
			String s = "create table "+ name + content ;
			stmt.executeUpdate(s);
		}catch (SQLException e)
		{ System.out.print("\n[ERROR] table " + name + " may already exist." );
		   //System.out.println("SQL table creation error. " + e.getMessage() ); 
		}catch (NullPointerException e)
		{ System.out.println("[ERROR] connection maybe not be properly setup.");
		}catch (Exception e)
		{ System.out.println("[ERROR] unknow error" + e); }
	}
	
	private static void drop_table(Statement stmt, String name)
	{
		String s = "drop table " + name + ";";
		try
		{
			stmt.executeUpdate(s);
		}catch (SQLException e)
		{ System.out.print("\n[ERROR] table " + name + " may not exist." );
		   //System.out.println("SQL table creation error. " + e.getMessage() ); 
		}catch (NullPointerException e)
		{ System.out.println("[ERROR] connection maybe not be properly set up.");
		}catch (Exception e)
		{ System.out.println("[ERROR] unknow error" + e); }
	}
	
	private static BufferedReader open_csv(String path, String filename)
	{
		try
		{
		String fileRow ;
		BufferedReader csvReader = new BufferedReader(new FileReader(path + filename));
		
		return csvReader ;
		}catch (FileNotFoundException e)
		{ System.out.println("[ERROR] did not found file " + filename + " in " + path ); 
		}catch (Exception e)
		{ System.out.println("[ERROR] unknow exception :" + e ); }
		return null;
	}
	
	// return null at the end of file or error
	private static String nextRow(BufferedReader csvReader)
	{
		String fileRow ;
		try
		{
			if ( (fileRow = csvReader.readLine()) != null)
				return fileRow;
			else
				return null;
		}catch (IOException e)
		{ System.out.println("[ERROR] input/output exception "); 
		}catch (NullPointerException e)
		{ System.out.println("[ERROR] csvReader maybe not be properly set up.");
		}catch (Exception e)
		{ System.out.println("[ERROR] unknow exception :" + e ); }
		return null;
	}
	
	private static void close_csv(BufferedReader csvReader)
	{
		if (csvReader == null)
			return;
		try
		{
			csvReader.close();
		}catch (IOException e)
		{ System.out.println("[ERROR] input/output exception "); 
		}catch (NullPointerException e)
		{ System.out.println("[ERROR] csvReader maybe not be properly set up.");
		}catch (Exception e)
		{ System.out.println("[ERROR] unknow exception :" + e ); }
		
	}

	// get user Choice
	private static int getChoice( int start, int end )
	{
		Scanner scan = new Scanner(System.in) ;
		String input ;
		int choice ;
		
		// loop for a valid input
		do
		{
			System.out.println("Please enter["+ start + "-" + end +"].");
			input = scan.nextLine();
			// System.out.println("input is " + input);
			try
			{
				choice = Integer.parseInt(input);
				//System.out.println("Choice is " + choice);
				
				if ( choice >= start && choice <= end )
					return choice;
				else
					System.out.println("[ERROR] Invalid input.");
			}catch (NumberFormatException e)
			{ System.out.println("[ERROR] Invalid input.");
			}catch (Exception e)
			{ System.out.println("[ERROR] Invalid input."); }
		} while (true) ;
		
	}

	// .csv file are assume to be correct in format, so this method should never throw exception
	private static int StringtoInt(String s)
	{
		try
		{
			int n = Integer.parseInt(s);
			return n ;
		}catch (NumberFormatException e)
		{ System.out.println("[ERROR] Invalid String to Int.");
		}catch (Exception e)
		{ System.out.println("[ERROR] Invalid String to Int error."); }
		// this value should never return
		return 123456789;
	}
	
	private static void printCount( Statement stmt, String tableName)
	{
		String query ;
		ResultSet rs ;
		int count ;
		String printName;
		
		try
		{
		query = "select count(*) from ";
		rs = stmt.executeQuery(query + tableName + ";");
		rs.next();
		count = rs.getInt(1);
		printName = tableName.substring(0,1).toUpperCase() + tableName.substring(1);
		System.out.println(printName +": " + count);
		}catch (SQLException e)
		{ System.out.println("[ERROR] SQL printCount error." + e ); }
	}
	
	public static void main(String[] args)
	{
		// connect to database
		Connection con = connect();
		
		Statement stmt = getStatement(con);
		PreparedStatement pstmt ;

		// setup user input
		int choice ;
		boolean flag = true ;
		
		do
		{
			System.out.println("Welcome! Who are you?");
			System.out.println("1. An administrator?");
			System.out.println("2. A passenager");
			System.out.println("3. A driver");
			System.out.println("4. A manager");
			System.out.println("5. None of the above");
			
			// get user input
			// int getInput( int start, int end );
			choice = getChoice (1,5);
			
			switch ( choice )
			{
				case 1 :
				{
					// An administrator

					boolean adminflag = true ;

					do {
					System.out.println("Administrator, what would you like to do");
					System.out.println("1. Create tables");
					System.out.println("2. Delete tables");
					System.out.println("3. Load data");
					System.out.println("4. Check data");
					System.out.println("5. Go back");

					choice = getChoice (1,5);
					
					switch ( choice )
					{
						case 1 :
						{
							// Administrator : Create tables
							System.out.print("Processing...");
							create_table(stmt, "vehicle",  "(id varchar(6),"+
											"model varchar(30),"+
											"seats integer,"+
											"primary key(id)"+
											");" );
							create_table(stmt, "driver",  "(id integer,"+
											"name varchar(30),"+
											"vehicle_id varchar(6) not null unique,"+
											"driving_years integer,"+
											"primary key(id),"+
											"foreign key(vehicle_id) references vehicle(id)"+
											");" );
							create_table(stmt, "passenger",  "(id integer,"+
											"name varchar(30),"+
											"primary key(id)"+
											");" );
							create_table(stmt, "taxi_stop",  "(name varchar(20),"+
											"location_x integer,"+
											"location_y integer,"+
											"primary key(name)"+
											");" );
							create_table(stmt, "request",  "(id integer,"+
											"passenger_id integer not null,"+
											"start_location varchar(20) not null,"+
											"destination varchar(20) not null,"+
											"model varchar(30),"+
											"passengers integer,"+
											"taken integer,"+
											"primary key(id),"+
											"foreign key (passenger_id) references passenger(id),"+
											"foreign key (start_location) references taxi_stop(name),"+
											"foreign key (destination) references taxi_stop(name)"+
											");" );
							create_table(stmt, "trip",  "(id integer,"+
											"driver_id integer not null,"+
											"passenger_id integer not null,"+
											"start_time varchar(19),"+
											"end_time varchar(19),"+
											"start_location varchar(20) not null,"+
											"destination varchar(20) not null,"+
											"fee integer,"+
											"primary key(id),"+
											"foreign key (driver_id) references driver(id),"+
											"foreign key (passenger_id) references passenger(id),"+
											"foreign key (start_location) references taxi_stop(name),"+
											"foreign key (destination) references taxi_stop(name)"+
											");" );
							
							System.out.println("Done! Tables are created!");
							
						}break;
						case 2 :
						{
							System.out.print("Processing...");
							drop_table(stmt, "trip");
							drop_table(stmt, "request");
							drop_table(stmt, "taxi_stop");
							drop_table(stmt, "passenger");
							drop_table(stmt, "driver");
							drop_table(stmt, "vehicle");
							System.out.println("Done! Tables are deleted");
						}break;
						case 3 :
						{
							Scanner sca = new Scanner(System.in) ;
							String input ;
							String path ;
							String fileRow ;		
							BufferedReader csvReader ;
							String data[];
	
							System.out.println("Please enter the folder path");
							input = sca.nextLine();
							path = "./" + input +"/";
							
							System.out.print("Processing...");
							
							csvReader = open_csv(path, "vehicles.csv");
							// fileRow = nextRow(csvReader);
							if(csvReader != null)
							while((fileRow = nextRow(csvReader)) != null)
							{
								data = fileRow.split(",");
								pstmt = getPreparedStatement(con, "insert into vehicle values(?,?,?);" ) ;
								try
								{
									pstmt.setString(1, data[0]);
									pstmt.setString(2, data[1]);
									pstmt.setInt(3, StringtoInt(data[2]));
									pstmt.execute();
								}catch (SQLException e)
								{ System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
								}catch (NullPointerException e)
								{ System.out.println("[ERROR] pstmt maybe not be properly set up."); }
							}
							close_csv(csvReader);

							csvReader = open_csv(path, "drivers.csv");
							// fileRow = nextRow(csvReader);
							if(csvReader != null)
							while((fileRow = nextRow(csvReader)) != null)
							{
								data = fileRow.split(",");
								pstmt = getPreparedStatement(con, "insert into driver values(?,?,?,?);" ) ;
								try
								{
									pstmt.setInt(1, StringtoInt(data[0]));
									pstmt.setString(2, data[1]);
									pstmt.setString(3, data[2]);
									pstmt.setInt(4, StringtoInt(data[3]));
									pstmt.execute();
								}catch (SQLException e)
								{ System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
								}catch (NullPointerException e)
								{ System.out.println("[ERROR] pstmt maybe not be properly set up."); }
							}
							close_csv(csvReader);

							csvReader = open_csv(path, "passengers.csv");
							// fileRow = nextRow(csvReader);
							if(csvReader != null)
							while((fileRow = nextRow(csvReader)) != null)
							{
								data = fileRow.split(",");
								pstmt = getPreparedStatement(con, "insert into passenger values(?,?);" ) ;
								try
								{
									pstmt.setInt(1, StringtoInt(data[0]));
									pstmt.setString(2, data[1]);
									pstmt.execute();
								}catch (SQLException e)
								{ System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
								}catch (NullPointerException e)
								{ System.out.println("[ERROR] pstmt maybe not be properly set up."); }
							}
							close_csv(csvReader);
						
							csvReader = open_csv(path, "taxi_stops.csv");
							// fileRow = nextRow(csvReader);
							if(csvReader != null)
							while((fileRow = nextRow(csvReader)) != null)
							{
								data = fileRow.split(",");
								pstmt = getPreparedStatement(con, "insert into taxi_stop values(?,?,?);" ) ;
								try
								{
									pstmt.setString(1, data[0]);
									pstmt.setInt(2, StringtoInt(data[1]));
									pstmt.setInt(3, StringtoInt(data[2]));
									pstmt.execute();
								}catch (SQLException e)
								{ System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
								}catch (NullPointerException e)
								{ System.out.println("[ERROR] pstmt maybe not be properly set up."); }
							}
							close_csv(csvReader);

							csvReader = open_csv(path, "trips.csv");
							// fileRow = nextRow(csvReader);
							if(csvReader != null)
							while((fileRow = nextRow(csvReader)) != null)
							{
								data = fileRow.split(",");
								pstmt = getPreparedStatement(con, "insert into trip values(?,?,?,?,?,?,?,?);" ) ;
								try
								{
									pstmt.setInt(1, StringtoInt(data[0]));
									pstmt.setInt(2, StringtoInt(data[1]));
									pstmt.setInt(3, StringtoInt(data[2]));
									pstmt.setString(4, data[3]);
									pstmt.setString(5, data[4]);
									pstmt.setString(6, data[5]);
									pstmt.setString(7, data[6]);
									pstmt.setInt(8, StringtoInt(data[7]));
									pstmt.execute();
								}catch (SQLException e)
								{ System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
								}catch (NullPointerException e)
								{ System.out.println("[ERROR] pstmt maybe not be properly set up."); }
							}
							close_csv(csvReader);

							System.out.println("Data is loaded!");

						}break;
						case 4 :
						{
							System.out.println("Numbers of records in each table:");
							printCount(stmt, "vehicle");
							printCount(stmt, "passenger");
							printCount(stmt, "driver");
							printCount(stmt, "trip");
							printCount(stmt, "request");
							printCount(stmt, "taxi_stop");
						}break;
						case 5 :
						{ adminflag = false ;
						}break;
						default :
						{	// default should never appear. Added here for debug. (admin)
							System.out.println("[ERROR] Enter default case. (admin)");}
					}
					} while (adminflag) ;
					
				}break;
				case 2 :
				{
					// A passenager
					// note : taken in request is integer
					// note : table trip take sequence of data in project file :
					// trip(id, driver_id, passenger_id, start_time, end_time,start_location, destination, fee)
					// which does not follow sequence of data in project file :
					// trip(id, driver_id, passenger_id, start_location, destination, start_time, end_time, fee)
				}break;
				case 3 :
				{
					// A driver
					// note : table trip take sequence of data in project file :
					// trip(id, driver_id, passenger_id, start_time, end_time,start_location, destination, fee)
					// which does not follow sequence of data in project file :
					// trip(id, driver_id, passenger_id, start_location, destination, start_time, end_time, fee)
				}break;
				case 4 :
				{
					// A manager
				}break;
				case 5 :
				{	// None of the above
					System.out.printf("Exiting application. Thanks for using.") ;
					flag = false ; }break;
				default :
					// default should never appear. Added here for debug.
					System.out.println("[ERROR] Enter default case.");
			}
		}while (flag) ;
		

	}
}