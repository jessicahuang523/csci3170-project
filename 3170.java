// testing
import java.io.*;
import java.util.*;
import java.sql.*;
import java.time.LocalDate;
import java.time.format.*;

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
							create_table(stmt, "request",  "(id integer AUTO_INCREMENT,"+
											"passenger_id integer not null,"+
											"start_location varchar(20) not null,"+
											"destination varchar(20) not null,"+
											"model varchar(30),"+
											"passengers integer,"+
											"taken integer,"+
											"driving_years integer,"+
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
					
                                    boolean passengerflag = true; 
                                    Scanner scan = new Scanner(System.in);
                                    do{
                                        System.out.println("Passenger, what would you like to do?");
                                        System.out.println("1. Request a ride");
                                        System.out.println("2. Check trip records");
                                        System.out.println("3. Go back");
                                        choice = getChoice(1,3);
                                        switch (choice){
                                            case 1: {
                                                
                                                //save and check the passenger id
                                                PreparedStatement pstmt0;
                                                PreparedStatement pstmt1;
                                                PreparedStatement pstmt2;
                                                PreparedStatement pstmt3;
                                                PreparedStatement pstmt4;
                                                PreparedStatement pstmt5;
                                                PreparedStatement pstmt6;
                                                ResultSet resultSet_0;
                                                ResultSet resultSet_1;
                                                ResultSet resultSet_2;
                                                ResultSet resultSet_3;
                                                ResultSet resultSet_4;  
                                                ResultSet resultSet_6;  
                                                
                                                
                                                
                                                // save and analyze the passenger id
                                                int passenger_ID;
                                                try{
                                                    while (true){
                                                        System.out.println("Please enter your ID.");
                                                        passenger_ID = scan.nextInt();
                                                        scan.nextLine();
                                                        String sql_id = "SELECT id from passenger where id = ?;";
                                                        pstmt0 = getPreparedStatement(con,sql_id);
                                                        pstmt0.setInt(1,passenger_ID);
                                                        resultSet_0 = pstmt0.executeQuery();
                                                        if (!resultSet_0.isBeforeFirst()){
                                                            System.out.println("[ERROR] ID not found");
                                                            continue;
                                                        }break;
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }
                                                   
                                                int passenger_No_passengers;
                                                // save and analyze the number of passengers
                                                    while(true){
                                                        System.out.println("Please enter the number of passengers.");
                                                        passenger_No_passengers = scan.nextInt();
                                                        scan.nextLine();
                                                        if (!(passenger_No_passengers <=8 && passenger_No_passengers>=1)){
                                                            System.out.println("[ERROR] Invalid number of passengers.");
                                                            continue;
                                                        }
                                                        break;
                                                    }
                                                
                                                
                                                // save and analyze the start location 
                                                String passenger_sloca = null ;
                                                try{
                                                    while(true){
                                                        System.out.println("Please enter the start location.");
                                                        passenger_sloca = scan.nextLine();
                                                        String sql_sloca = "SELECT name from taxi_stop where name = ?;";
                                                        pstmt1 = getPreparedStatement(con,sql_sloca);
                                                        pstmt1.setString(1,passenger_sloca);
                                                        resultSet_1 = pstmt1.executeQuery();
                                                        if (!(resultSet_1.isBeforeFirst())){
                                                            System.out.println("[ERROR] Start Location not found.");
                                                            continue;
                                                        }  
                                                        break;
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }       
                                                
                                                 // save and analyze the destination
                                                  String passenger_dest;
                                                  try{
                                                    while(true){
                                                        System.out.println("Please enter the destination.");
                                                        passenger_dest = scan.nextLine();
                                                        String sql_dest = "SELECT name from taxi_stop where name = ?;";
                                                        pstmt2 = getPreparedStatement(con,sql_dest);
                                                        pstmt2.setString(1,passenger_dest);
                                                        resultSet_2 = pstmt2.executeQuery();
                                                        if (!(resultSet_2.isBeforeFirst())){
                                                            System.out.println("[ERROR] Destination not found.");
                                                           continue;
                                                        }else if (passenger_dest.equals(passenger_sloca) ){
                                                                  System.out.println("[ERROR] Destination and start location should be different.");  
                                                                  continue;
                                                        }   
                                                        break;
                                                }
                                                  }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }
                                                //save and analyze the model
                                                String passenger_model_1;
                                                try{
                                                    while(true){
                                                        System.out.println("Please enter the model. (press enter to skip)");
                                                        passenger_model_1 = scan.nextLine();
                                                        String sql_model = "SELECT * from vehicle where model LIKE \"" + passenger_model_1 + "%\"";
                                                        pstmt3 = getPreparedStatement(con,sql_model);
                                                        resultSet_3 = pstmt3.executeQuery();
                                                        if ( passenger_model_1.isEmpty()){   
                                                            break;
                                                        }
                                                        else if (!resultSet_3.isBeforeFirst()){
                                                            System.out.println("[ERROR] Model not found.");
                                                            continue;
                                                        }                     
                                                        break;
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }
                                                
                                                //save the driving years 
                                                int passenger_driver_years;
                                                System.out.println("Please enter the minimum driving years of the driver. (press enter to skip)");
                                                String passenger_mini_dyears = scan.nextLine();  
                                                if (passenger_mini_dyears.isBlank()) {
                                                    passenger_driver_years = 0;                               
                                                }else{
                                                    passenger_driver_years = Integer.parseInt(passenger_mini_dyears);
                                                }
                                                
                                                
                                                //check if passenger has open request or not first    
                                                try{
                                                    String check_open_request = "select * from request r where r.passenger_id =" + passenger_ID + " and taken = 0;";
                                                    pstmt6 = getPreparedStatement(con,check_open_request);
                                                    resultSet_6 = pstmt6.executeQuery();
                                                    if ((resultSet_6.isBeforeFirst())){
                                                        System.out.println("You have an open request already.");
                                                        break;
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }

                                                // create a request  
                                                try{
                                                    String count_request_string = "select count(*) from vehicle v, driver d where v.seats >="+ passenger_No_passengers +" and "
                                                            + "d.vehicle_id=v.id and d.driving_years >=" + passenger_driver_years +" and v.model LIKE \"" +passenger_model_1 + "%\""; 
                                                    pstmt4 = getPreparedStatement(con,count_request_string);
                                                    resultSet_4 = pstmt4.executeQuery();
                                                    resultSet_4.next();
                                                    int count_requests = resultSet_4.getInt("count(*)");

                                                    // if count<0 continue to another iteration.
                                                    if (count_requests<1){
                                                        System.out.println("No records found. Please adjust the criteria.");
                                                        continue;
                                                    }
                                                    // insert new request.
                                                    else{   

                                                        String sql_request = "insert into request (passenger_id, start_location, destination, model,"
                                                                + "passengers, taken) VALUES (?,?,?,?,?,?)";					
                                                        pstmt5 = getPreparedStatement(con,sql_request);
                                                        pstmt5.setInt(1,passenger_ID);
                                                        pstmt5.setString(2,passenger_sloca);
                                                        pstmt5.setString(3,passenger_dest);
                                                        pstmt5.setString(4,passenger_model_1);
                                                        pstmt5.setInt(5,passenger_No_passengers );
                                                        pstmt5.setInt(6, 0);       
                                                        pstmt5.executeUpdate();
                                                        System.out.println("Your request is placed."+count_requests+" drivers are able to take the request.");
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }
                                                try{
							pstmt0.close();
							resultSet_0.close();
							pstmt1.close();
							resultSet_1.close();
							pstmt2.close();
							resultSet_2.close();
							pstmt3.close();
							resultSet_3.close();
							pstmt4.close();
							resultSet_4.close();
							pstmt5.close();
							pstmt6.close();
							resultSet_6.close();
						}catch (SQLException e)
                                                        {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                        break;
					    	}
                                                     
                                            }break;
                                            
                                            case 2:{
                                                PreparedStatement pstmt1;
                                                ResultSet resultSet_1;
                                                PreparedStatement pstmt2;
                                                ResultSet resultSet_2;
                                                PreparedStatement pstmt0;
                                                ResultSet resultSet_0;
                                                
                                                // save and analyze the passengers id
                                                int passenger_ID;
                                                try{
                                                    while (true){
                                                        System.out.println("Please enter your ID.");
                                                        passenger_ID = scan.nextInt();
                                                        scan.nextLine();
                                                        String sql_id = "SELECT id from passenger where id = ?;";
                                                        pstmt0 = getPreparedStatement(con,sql_id);
                                                        pstmt0.setInt(1,passenger_ID);
                                                        resultSet_0 = pstmt0.executeQuery();
                                                        if (!resultSet_0.isBeforeFirst()){
                                                            System.out.println("[ERROR] ID not found");
                                                            continue;
                                                        }break;
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }
                                                    
                                                // check if the date is in correct form
                                                String passenger_sdate;
                                                while (true){
                                                    try{
                                                           System.out.println("Please enter the start date.");
                                                           passenger_sdate = scan.nextLine();                                                
                                                           LocalDate.parse(passenger_sdate, DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT));
                                                           break;
                                                    } catch (DateTimeParseException e){
                                                           System.out.println("[ERROR] not correct form of date");
                                                           }
                                                }
                                                
                                                // check if the date is in correct form
                                                String passenger_edate;
                                                while (true){
                                                    try{
                                                           System.out.println("Please enter the end date.");
                                                           passenger_edate = scan.nextLine();                                               
                                                           LocalDate.parse(passenger_edate, DateTimeFormatter.ofPattern("uuuu-MM-dd").withResolverStyle(ResolverStyle.STRICT));
                                                           break;
                                                    } catch (DateTimeParseException e){
                                                           System.out.println("[ERROR] not correct form of date");
                                                           }
                                                }
                                                
                                                // save and analyze the passengers destination
                                                String passenger_dest;
                                                try{
                                                    while(true){
                                                        System.out.println("Please enter the destination.");
                                                        passenger_dest = scan.nextLine();
                                                        String sql_dest = "SELECT name from taxi_stop where name = ?;";
                                                        pstmt2 = con.prepareStatement(sql_dest);
                                                        pstmt2.setString(1,passenger_dest);
                                                        resultSet_2 = pstmt2.executeQuery();
                                                        if (!(resultSet_2.isBeforeFirst())){
                                                            System.out.println("[ERROR] Destination not found.");
                                                           continue;
                                                        }
                                                        break;
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }
                                                
                                                
                                                //return the finished trip records
                                                try{
                                                    String psql = "SELECT t.id, d.Name, d.vehicle_ID, v.model, t.start_time, t.end_time, t.fee, t.start_location, t.destination "
                                                            + "FROM trip t, driver d, vehicle v "
                                                            + "WHERE t.driver_id=d.id and d.vehicle_id=v.id and t.passenger_id =" + passenger_ID + "  and t.start_time >= \'"+  passenger_sdate 
                                                            + "\' and t.end_time <= \'" + passenger_edate  + "\' and t.destination = \'" + passenger_dest + "\';";
                                                    pstmt1 = con.prepareStatement(psql);            
                                                    resultSet_1 = pstmt1.executeQuery();
                                                    if (!resultSet_1.isBeforeFirst()){
                                                        System.out.println("No records found.");
                                                    }else{
                                                        while (resultSet_1.next()){
                                                            System.out.println("Trip_id, Driver Name, Vehicle ID, Vehicle Model, Start, End, Fee, Start Location, Destination");
                                                            System.out.print(resultSet_1.getInt(1)+"\t");
                                                            System.out.print(resultSet_1.getString(2)+"\t");
                                                            System.out.print(resultSet_1.getString(3)+"\t");
                                                            System.out.print(resultSet_1.getString(4)+"\t");
                                                            System.out.print(resultSet_1.getDate(5)+"\t");
                                                            System.out.print(resultSet_1.getDate(6)+"\t");
                                                            System.out.print(resultSet_1.getInt(7)+"\t");
                                                            System.out.print(resultSet_1.getString(8)+"\t");
                                                            System.out.print(resultSet_1.getString(9));
                                                            System.out.println();
                                                        }
                                                    }
                                                }catch(SQLException e)
                                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                    break;
                                                }catch (NullPointerException e)
                                                    {System.out.println("[ERROR] pstmt0 maybe not be properly set up.");
                                                    break;
                                                }
						
						try{
							pstmt0.close();
							resultSet_0.close();
							 pstmt1.close();
							resultSet_1.close();
							 pstmt2.close();
							resultSet_2.close();
						}catch (SQLException e)
                                                        {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                                        break;
					    	}
                                            }break;
                                            
                                            case 3: {
                                                passengerflag = false;
                                            }break;
                                            default: {
                                                System.out.println("[ERROR] Enter default case. (admin)");}
                                            }
                                        }while(passengerflag);
                                
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
					boolean driverflag = true;
					Scanner scan = new Scanner(System.in);

					do{
						System.out.println("Driver, what would you like to do?");
						System.out.println("1. Search requests");
						System.out.println("2. Take a request");
						System.out.println("3. Finish a trip");
						System.out.println("4. Go back");

						int driver_choice = getChoice (1,4);

						switch (driver_choice){
							case 1: { // search request
								
								int driver_id;
								int coor_x;
								int coor_y;
								int max_dis;
								PreparedStatement pstmt_id;
								PreparedStatement pstmt_search;
								ResultSet resset_id;
								ResultSet resset_search;

								// check valid driver id
								try{
                                	while (true){
                                        System.out.println("Please enter your ID.");
                                        driver_id = scan.nextInt();
                                        String find_id = "SELECT id from driver where id = ?;";
                                        pstmt_id = getPreparedStatement(con,find_id);
                                        pstmt_id.setInt(1,driver_id);
                                        resset_id = pstmt_id.executeQuery();
                                        if (!resset_id.isBeforeFirst()){
                                            System.out.println("[ERROR] ID not found.");
                                            continue;
                                        }break;
                                    }
                                }catch(SQLException e)
                                    {System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
                                    break;
                                }catch (NullPointerException e)
                                    {System.out.println("[ERROR] pstmt_id maybe not be properly set up.");
                                    break;
                                }
								
								// get coordinates
								System.out.println("Please enter the coordinates of your location.");
								coor_x = scan.nextInt();
								coor_y = scan.nextInt();

								// get max distance
								System.out.println("Please entere the maximum distance from you to the passenger.");
								max_dis = scan.nextInt();

								// search for qualified and open requests in the distance
								try{
									String search_sql = "SELECT r.id, p.name, r.passengers, r.start_location, r.destination "+
														"FROM request r, driver d, vehicle v, taxi_stop ts, passenger p "+
														"WHERE d.id=" + driver_id + " and d.vehicle_id=v.id and v.seats>=r.passengers and r.taken=0 and v.model LIKE \'r.model%\' and d.driving_years>=r.driving_years and r.start_location=ts.name and "+ max_dis + ">=ABS(" + coor_x + "-ts.location_x)+ABS("+ coor_y +"-ts.location_y) and r.passenger_id=p.id;";
									pstmt_search = getPreparedStatement(con, search_sql);
									resset_search = pstmt_search.executeQuery();
									if (!resset_search.isBeforeFirst()){
										System.out.println("No records found.");
									}else{
										while (resset_search.next()){
											System.out.println("request ID, passenger name, num of passengers, start location, destination\n");
											System.out.print(resset_search.getInt(1)+"\t");
											System.out.print(resset_search.getString(2)+"\t");
											System.out.print(resset_search.getInt(3)+"\t");
											System.out.print(resset_search.getString(4)+"\t");
											System.out.print(resset_search.getString(5)+"\t");
											System.out.println();
										}
									}
								}catch(SQLException e)
									{System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
									break;
								}catch (NullPointerException e)
									{System.out.println("[ERROR] pstmt_search maybe not be properly set up.");
									break;
								}
								
							} break;
							case 2:{
								System.out.println("Please enter your ID.");
								int driver_id = scan.nextInt();
								System.out.println("Please enter the request ID.");
								int request_id = scan.nextInt();
								//temporary
								System.out.println(driver_id);
								System.out.println(request_id);
							} break;
							case 3:{
								System.out.println("Please enter your ID.");
								int driver_id = scan.nextInt();
								//temporary
								System.out.println(driver_id);
							} break;
							case 4:{
								driverflag = false;
							} break;
						}
					} while(driverflag);
					// note : table trip take sequence of data in project file :
					// trip(id, driver_id, passenger_id, start_time, end_time,start_location, destination, fee)
					// which does not follow sequence of data in project file :
					// trip(id, driver_id, passenger_id, start_location, destination, start_time, end_time, fee)

				}break;
				case 4 :
				{
					// A manager
					boolean managerflag = true;
					Scanner scan = new Scanner(System.in);

					do{
						System.out.println("Manager, what would you like to do?");
						System.out.println("1. Find trips");
						System.out.println("2. Go back");

						int manager_choice = getChoice (1,2);

						switch(manager_choice){
							case 1: {
								int min_dis;
								int max_dis;
								PreparedStatement trips_pstmt;
								ResultSet resset_trips;

								//get min and max
								System.out.println("Please enter the minimum traveling distance.");
								min_dis = scan.nextInt();
								System.out.println("Please enter the maximum traveling distance.");
								max_dis = scan.nextInt();

								//find trip
								try{
									String trips_sql = "SELECT t.id, d.name, p.name, t.start_location, t.destination " +
														"FROM trip t, taxi_stop ts1, taxi_stop ts2, driver d, passenger p " +
														"WHERE t.start_location=ts1.name and t.destination=ts2.name and "+ min_dis + "<=ABS(ts1.location_x-ts2.locationi_x)+ABS(ts1.location_y-ts2.location_y) and "+ max_dis + ">=ABS(ts1.location_x-ts2.locationi_x)+ABS(ts1.location_y-ts2.location_y) and t.driver_id=d.id and t.passenger_id=p.id;";
									trips_pstmt = getPreparedStatement(con, trips_sql);            
									resset_trips = trips_pstmt.executeQuery();
									if (!resset_trips.isBeforeFirst()){
										System.out.println("No records found.");
									}else{
										while (resset_trips.next()){
											System.out.println("trip id, driver name, passenger name, start location, destination, duration\n");
											System.out.print(resultSet_1.getInt(1)+"\t");
											System.out.print(resultSet_1.getString(2)+"\t");
											System.out.print(resultSet_1.getString(3)+"\t");
											System.out.print(resultSet_1.getString(4)+"\t");;
											System.out.print(resultSet_1.getString(5)+"\t");
											System.out.println();
										}
									}
								}catch(SQLException e)
									{System.out.println("[ERROR] SQL exception in pstmt.set. " + e); 
									break;
								}catch (NullPointerException e)
									{System.out.println("[ERROR] trips_pstmt maybe not be properly set up.");
									break;
								}
								
							} break;
							case 2: {
								managerflag = false;
							} break;
						}

					} while(managerflag);

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
