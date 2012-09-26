/*
 * Program for log file extraction and loading values into database for a print server.
 * Applies to Windows 7, Windows 2008 server.
 * The printed document's details are stored in windows logs.
 * Power shell commands can be used to extract log.
 * To execute powershell commands the command prompt can be used, which can be used to produce a CSV file.
 * CSV file is parsed and loaded into the database.
 */

package logExtraction;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;
/** Used for apache logging */
import org.apache.log4j.Logger;
/** Used for parsing the columns of CSV */
import com.douglasjose.tech.csv.Csv;
import com.douglasjose.tech.csv.CsvFactory;

/**
 * @author Varadhan
 * Date: 15-09-2012
 */

/** The CreatePrinterLog class */
public class CreatePrinterLogCSV {

	/** Using apache log4j */
	static Logger slogger = Logger.getLogger(CreatePrinterLogCSV.class);
	
	/** Configuration for database and where to store CSV files are given in propertiesFile */
	static final String propertiesFile = "printerLog.properties";
	
	/** .bat file is created and run so that java can wait until it completes.
	 * Otherwise java does not wait for piped process to exit
	 */
	static final String powershellFile = "printerLog.ps1";
	
	/** Default CSV file location set to current directory */
	static String csvFileLocation = ".";
	
	/** Parameters required for setting up database connection */
	private static String db;
	private static String connectionString;
	private static String dbUsername;
	private static String dbPassword;
	private static String dbName;
	private static String schema;
	private Connection connection = null;	

	/** In case transaction fails commit is not done on data into database. */
	private boolean transactionFailed = false;
	
	/** Used to prepare statement for query of db */
	private PreparedStatement queryStatement;
	
	/** Used to obtain results of query from database */
	private ResultSet queryResult;
	
	/** To avoid checking if month table exists each time in database. */
	private String currentMonth = "";
	
	/** Details that will be extracted from log and stored in database */
	class PrintingDetails {
		String regNo;
		String documentName;
		String printerName;
		Timestamp printTime;
		int pages;
		double cost;
		int bannerID;
	}
	
	/** Gets executed when program starts */
	static{
		slogger.info("Starting to run maintenance");
		Properties logProperties = new Properties();
		try{
			// Loading values from properties file
			slogger.info("Loading properties file");			
			logProperties.load(new FileInputStream(propertiesFile));
			csvFileLocation = logProperties.getProperty("log.location");
			db=logProperties.getProperty("db");			
			dbUsername = logProperties.getProperty("db."+db+".username");
			dbPassword = logProperties.getProperty("db."+db+".password");
			dbName = logProperties.getProperty("db."+db+".name");
			schema = logProperties.getProperty("db.schema");
			connectionString = logProperties.getProperty("db."+db+".url")+dbName;
						
		}catch(IOException ioe){
			slogger.warn( "Error loading properties file. "
					+ioe.getMessage());
		}
	}
	
	/** Setting up database connection */
	private void createDatabaseConnection(){
		if(connection == null){
			slogger.info("Trying to create database connection.");		
			try {
				Class.forName("net.sourceforge.jtds.jdbc.Driver");
				connection = DriverManager.getConnection(connectionString,dbUsername,dbPassword);
				connection.setAutoCommit(false);
				slogger.info("Connected with connection string "+connectionString);
			} catch (SQLException | ClassNotFoundException e) {
				slogger.fatal( "Cannot create database connection."
						+"\n"
						+e.getMessage()
						);
				//TODO: Ask sir on what must be done for abnormal cases.
				System.exit(1);
			}
		}
	}
	
	/** The default constructor that creates connection to database during object creation */
	public CreatePrinterLogCSV(){
		createDatabaseConnection();
	}
	
	/** Used to get the last date when the log extraction was done from table stored in database */
	 public String getPreviousExecutionDate(){
		//TODO: Ask sir on what table stores prev execution date.
		String prevExecQuery = "SELECT * FROM "+schema+".TABLE";
		try {
			queryStatement = connection.prepareStatement(prevExecQuery);
			queryResult = queryStatement.executeQuery();
			if(queryResult.next()){
				return queryResult.getTimestamp("previousLogExtraction").toString();
			}
		} catch (SQLException e) {
			slogger.fatal( "Unable to get previous date of execution. "
					+e.getMessage());
		}
		return null;
	}
	 
	private void createLogFileUptoYesterday(Date startDate){
		Calendar yesterday = GregorianCalendar.getInstance();
		yesterday.setTime(getYesterDay());		
		createLogFile(startDate, yesterday.getTime());
	}
	
	/**
	 * Used to create log files from windows log and load it into database
	 * Command to get csv file from windows
	 * get-winevent -FilterHashTable @{ logname = 'Microsoft-Windows-PrintService/Operational';StartTime = '"+givenDate+" 12:00:01 AM'; EndTime = '"+yesterday+" 13:59:00'; ID = 307 ;} | ConvertTo-csv| Out-file "+csvFileLocation+"\\"+givenDate.replaceAll("/", "-")+".csv\""
	 * In case event is to be executed as powershell file then permissions have to be set for executing file. Refer help manual of powershell.
	 */
	public void createLogFile(Date startDate,Date endDate){
		if(startDate.after(endDate)){
			slogger.warn("Start date should be less than end date.");
			return;
		}
		if(endDate.after(new Date())){
			slogger.warn("End date greater than today resetting end date.");
			endDate = new Date();
		}
		Runtime commandPrompt = Runtime.getRuntime();		
		Calendar cal = GregorianCalendar.getInstance();				
		cal.setTime(startDate);		
		String givenDate = cal.get(Calendar.DATE)+"/"+(cal.get(Calendar.MONTH)+1)+"/"+cal.get(Calendar.YEAR);
		cal.setTime(endDate);
		String beforeDay = cal.get(Calendar.DATE)+"/"+(cal.get(Calendar.MONTH)+1)+"/"+cal.get(Calendar.YEAR);
		String file =  csvFileLocation+"\\"+givenDate.replaceAll("/", "-")+"_to_"+beforeDay.replaceAll("/", "-")+".csv";
		File f = new File(file);
		String completedFile = "completed.txt";
		slogger.info("Creating log for "+givenDate);		
		try {
			File completedSignal = new File(completedFile);
			if(completedSignal.exists())
				completedSignal.delete();
			String command ="get-winevent -FilterHashTable @{ logname = 'Microsoft-Windows-PrintService/Operational';StartTime = '"+givenDate+" 12:00:01 AM'; EndTime = '"+beforeDay+" 23:59:59 ';  ID = 307 ;} | ConvertTo-csv| Out-file "+ file
					+";new-item "+completedFile+"  -type file;";			
			commandPrompt.exec("powershell -NonInteractive -Command "+ command);
			while(!completedSignal.exists())
				;
			completedSignal.delete();
			
		} catch (IOException e) {
			slogger.fatal( "Unable to execute command and create log file. "
					+ e.getMessage());
			System.exit(1);
		} catch (Exception e) {
			slogger.fatal( "Unable to execute command and create log file. Exiting.."
					+ e.getMessage());
			System.exit(1);
		}
		parseCSVFile(f);
	}
	
	/** Get yesterdays date */
	private Date getYesterDay(){
		Date today = new Date();
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTime(today);
		cal.add(Calendar.DATE, -1);
		return cal.getTime();
	}
	
	/** Start creating extracting log file for yesterday */
	public void createLogFileYesterday(){		
		createLogFileUptoYesterday(getYesterDay());
	}
	
	/** CSV file contains some non printable characters that must be stripped. */
	private String stripNonPrintable(String s){	
		char[] oldChars = new char[s.length()];
	    s.getChars(0, s.length(), oldChars, 0);
	    char[] newChars = new char[s.length()];
	    int newLen = 0;
	    for (int j = 0; j < s.length(); j++) {
	        char ch = oldChars[j];
	        if (ch >= ' ') {
	            newChars[newLen] = ch;
	            newLen++;
	        }
	    }
	    s = new String(newChars, 0, newLen);
	    return s;
	}
	
	
	/**
	 * getLogDataFromContent
	 * 
	 * Example format of content
	 * Document 1790, Microsoft Word - VIPUL_KUMAR_ECE owned by 108109109 on \\10.1.34.21 was printed on A4-4515x through port 10.0.0.43_2.  Size in bytes: 201625. Pages printed: 3. No user action is required.
	 * Here sometimes IP address may be replaced by PC name in case DNS is used. So no point in parsing ip address
	 * The Document name can also be arbitrarily long as files printed driectly from URL's such as attachment in gmail may have.
	 * Hence the database must have a huge value for storing file names. (Field size of 200 may be small.)
	 * Other information contained in the CSV file are mostly not required.
	 * 
	 * @param the content to be parsed and timestamp obtained from CSV file
	 * @return the details extracted from content and encapsulated as PrintingDetails
	 */
	@SuppressWarnings("deprecation")
	private PrintingDetails getLogDataFromContent(String content,String date){
		PrintingDetails temp = new PrintingDetails();
		String ownedBy = "owned by ";
		String wasPrintedOn = "was printed on ";
		String pagesPrinted = "Pages printed: ";		
		String banner;
		//System.out.println(content);
		String backUpContet = new String(content);
		
		// Get document <id>
		banner = content.substring("Document ".length(),content.indexOf(","));		
		
		// Strip out "Document <id>" from content 
		content = content.substring(content.indexOf(",")+1);
		
		// Name of document printed starts immediately
		int startIndexOwnedBy = content.lastIndexOf(ownedBy);		
		
		int startIndexPagesPrinted = content.lastIndexOf(pagesPrinted);
		
		int startIndexWasPrintedOn = content.lastIndexOf(wasPrintedOn);
		
		
		
		try{
			temp.bannerID = Integer.parseInt(banner);
			temp.regNo = content.substring((startIndexOwnedBy+ownedBy.length()));
			temp.regNo = temp.regNo.substring(0, temp.regNo.indexOf("on")-1);
			temp.documentName = content.substring(0, startIndexOwnedBy - 1);
			
		}catch(Exception e){
			slogger.fatal("Exception caught.. "
					+e.getMessage());
			slogger.fatal("Error occured on content "+backUpContet);
			slogger.info("Values parsed: banner "+temp.bannerID+" regNo "+temp.regNo+" Doc "+temp.documentName);
			transactionFailed = true;
			return null;
		}
		String pages = content.substring(startIndexPagesPrinted + pagesPrinted.length());
		pages = pages.substring(0, pages.indexOf("."));
		
		temp.pages = Integer.parseInt(pages);
		
		// Get the substring that starts with the printer name and then strip out printer name
		temp.printerName = content.substring(startIndexWasPrintedOn+wasPrintedOn.length());		
		temp.printerName = temp.printerName.substring(0, temp.printerName.indexOf(" through port"));
			
		// Uncomment to check what is printed.
		/*
		System.out.println("Documet name: "+temp.documentName+"\nRegno: "+temp.regNo);
		System.out.println("Pages: "+temp.pages+ "\nPrinter Name: "+temp.printerName);
		System.out.println("Date printed: "+date);
		*/
		
		date = date.replaceAll("-", "/");
		temp.printTime = new Timestamp(new Date(date).getTime());
		
		return temp;
	}
	/**
	 * parseCSVFile
	 * 
	 * Used to parse the CSV file obtained from windows logs using jcsv library in google projects 
	 * @param file
	 */
	private void parseCSVFile(File file){		
		slogger.info("Trying to parse CSV file.");
		Csv csv = CsvFactory.createOfficeCsv();
		FileInputStream csvInput = null;
		try {
			csvInput = new FileInputStream (file);
			csv.load(csvInput);
			
			// If number of rows less than 3 there is no content.
			// The first line contains the type information and second line contains column headers
			
			if(csv.getRows() < 3 ){
				slogger.warn("No content to parse in file");
				return;
			}
			
			String content,date;
			
			// Neglect first 3 rows
			for (int i = 4; i < csv.getRows(); i++) {			    
			        content = csv.get(i, 0);
			        content = stripNonPrintable(content).trim();
			        
			        // Sometimes line may have only non-printable chars in that case all are striped and blank space remains
			        if(content.length() > 2 ){				        
				        date = csv.get(i, 16);
				        date = stripNonPrintable(date);
				        PrintingDetails studentDetails = getLogDataFromContent(content,date);
				        loadPrintingDetailsOfSudentToDB(studentDetails);
			        }			        
			}
		} catch (IOException e1) {
			slogger.fatal("IO Error. "
					+e1.getMessage());
			transactionFailed = true;
		}
		if( csvInput != null )
			try {
				csvInput.close();
			} catch (IOException e) {
				slogger.warn("Unable to close file. "
						+e.getMessage());
			}
	}
	
	/**
	 * Use a sample file to test working of program
	 */
	public void ParseCSVAndLoadToDB(String fileName){
		slogger.info("Trying to test parsing sample file.. "+fileName);
		File csvFile = new File(csvFileLocation+"\\"+fileName);
		if(csvFile.exists()){
			parseCSVFile(csvFile);
		}else{
			slogger.fatal("File does not exist "+csvFileLocation+"\\"+fileName);
		}
	}
	
	/**
	 * Close all connections to database and other files. Commit changes to database.
	 */
	private void cleanUp(){
		
		try {
			if( !transactionFailed ){
				slogger.info("Comitting transaction");
				connection.commit();
			}else{				
				slogger.fatal("Aborting transaction commit due to failed transactions.");
				connection.rollback();
			}
		} catch (SQLException e1) {
			slogger.fatal("Cannot commit changes. "
					+e1.getMessage());
		}
		slogger.info("Closing all resources");
		if ( connection != null ){
			try {
				connection.close();
			} catch (SQLException e) {
				slogger.warn("Cannot close database connection. "
						+e.getMessage());
			}
		}
		slogger.info("Completing service maintenance.");
	}
	
	/**
	 * Used to insert the values for student printout details into database
	 * @param details
	 */
	public void loadPrintingDetailsOfSudentToDB(PrintingDetails details){	
		/**
		 * HOW TO: Get print date to check if *****temporary***** month table exists.
		 * If exists enter values into that. If not create a table.
		 * For each entry inserted in month table if rollNo exists in CSGLED update that
		 * otherwise create a new entry in CSGLED. 
		 */		
		if(details.regNo.length() >= 11)
			details.regNo = details.regNo.substring(0,10);
		if(details.documentName.length() >= 100)
			details.documentName = details.documentName.substring(0,99);
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTime(details.printTime);
		Date dt = cal.getTime();
		//System.out.println(dt+ " "+details.printTime);
		String date = dt.toString();
		String monthMMM = date.substring(4,7);
		String yearYYYY = date.substring(date.length()-4);
		//System.out.println("month: "+monthMMM+" year: "+yearYYYY);
		
		String monthTableExistsQuery = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE ?";
		String createMonthTableQuery = "CREATE TABLE "+
				schema+".TEMP"+monthMMM+yearYYYY+ "("
					+"	rollno nvarchar(11),	"
					+"	cdate nvarchar(10),		"
					+"	A2 nvarchar(50),		"
					+"	pages float(8),			"
					+"	A11 nvarchar(25),		"
					+"	charges float(8),		"
					+"	line float(8),			"
					+"  filename nvarchar(100)  "
				+");";
		String csgledEntryExistsQuery = "SELECT rollno, mo, charges FROM "+schema+".csgled WHERE rollno = ? AND mo = ?";
		String csgledCreateEntryQuery = "INSERT INTO "+schema+".csgled (rollno, remarks, drcr, charges, mon, mo) values (?,?,?,?,?,?)";
		String updateCsgledEntryQuery = "UPDATE "+schema+".csgled SET charges=? WHERE rollno=? AND mo=?";
		// Month query must be inserted into a temporary table which will be finalized using frontend. Hence temp included in table name
		String insertMonthEntry = "INSERT INTO "+schema+".TEMP"+monthMMM+yearYYYY+" (rollno,cdate,A2,pages,A11,charges,line,filename) VALUES (?,?,?,?,?,?,?,?)";
		String pageCostQuery = "SELECT costperpage, costFirstpage, additionalCost FROM "+schema+".costs WHERE printername = ?";
		
		
		float charges = 0.0f;
		
		try{
			
			queryStatement = connection.prepareStatement(pageCostQuery);
			queryStatement.setString(1, details.printerName);
			queryResult = queryStatement.executeQuery();
			if(queryResult.next()){
				float costPerPage = queryResult.getFloat("costperpage");
				float CostFirstPage = queryResult.getFloat("costFirstpage");
				float additionalCost = queryResult.getFloat("additionalCost");
				charges = CostFirstPage + additionalCost + (costPerPage * details.pages);
			}
			queryStatement.close();
			queryResult.close();
			
			// CHECK IF MONTH TABLE EXISTS
			if(!currentMonth.equals(monthMMM+yearYYYY)){
				queryStatement = connection.prepareStatement(monthTableExistsQuery);
				queryStatement.setString(1, "TEMP"+monthMMM+yearYYYY);
				queryResult = queryStatement.executeQuery();
				if(!queryResult.next()){
					queryStatement = connection.prepareStatement(createMonthTableQuery);				
					queryStatement.executeUpdate();
				}else{
					
				}
				queryResult.close();
				queryStatement.close();
				currentMonth = monthMMM + yearYYYY;
			}
			
			queryStatement = connection.prepareStatement(insertMonthEntry);			
			queryStatement.setString(1, details.regNo);
			queryStatement.setString(2, details.printTime.toString().substring(0,10));
			queryStatement.setString(3, details.printTime.toString().substring(11));
			queryStatement.setFloat(4,details.pages);
			if(details.printerName.length() >24 )
				details.printerName = details.printerName.substring(0,24);
			queryStatement.setString(5, details.printerName);
			queryStatement.setFloat(6, charges);
			queryStatement.setFloat(7, details.bannerID);
			queryStatement.setString(8, details.documentName);
			queryStatement.executeUpdate();
			queryStatement.close();
			
			
			// CHECK IF PERSON HAS ALREADY TAKEN ANY PRINTOUT IN THAT MONTH
			queryStatement = connection.prepareStatement(csgledEntryExistsQuery);
			queryStatement.setString(1, details.regNo);
			queryStatement.setString(2, monthMMM+yearYYYY);
			queryResult = queryStatement.executeQuery();
			
			if(!queryResult.next()){
				queryStatement = connection.prepareStatement(csgledCreateEntryQuery);
				queryStatement.setString(1, details.regNo);
				queryStatement.setString(2, "Printout charges for "+monthMMM+" "+yearYYYY);
				queryStatement.setString(3, "dr");
				queryStatement.setFloat(4, charges);
				@SuppressWarnings("deprecation")
				int mon = Integer.parseInt(yearYYYY) * 100 + dt.getMonth();
				queryStatement.setFloat(5, mon);
				queryStatement.setString(6, monthMMM+yearYYYY);				
				queryStatement.executeUpdate();
			}else{
				float existingCharges = queryResult.getFloat("charges");
				charges += existingCharges;
				queryStatement = connection.prepareStatement(updateCsgledEntryQuery);
				queryStatement.setFloat(1, charges);
				queryStatement.setString(2, details.regNo);
				queryStatement.setString(3, monthMMM+yearYYYY);
				queryStatement.executeUpdate();
			}
			queryStatement.close();
			queryResult.close();
			/*
			System.out.println("Timestamp: "+details.printTime.toString());		
			System.out.println("Documet name: "+details.documentName+"\nRegno: "+details.regNo);
			System.out.println("Pages: "+details.pages+ "\nPrinter Name: "+details.printerName);
			*/
			
		} catch (Exception e) {
			slogger.fatal("Problem with loading data into database."
					+e.getMessage());
			slogger.info("Values of incorrect data: "+details.regNo+" "+details.printerName+" "+details.bannerID+" "+details.documentName
					+" "+details.printTime);
			//e.printStackTrace();
			//TODO: Ask sir on what policy must be used for exit
			System.exit(1);
			transactionFailed = true;
		}
	}
	
	public static void main(String[] args){
		CreatePrinterLogCSV pLog = new CreatePrinterLogCSV();
		
		Calendar cal = GregorianCalendar.getInstance();
		cal.setTime(new Date());
		cal.add(Calendar.DATE, -2);
		//pLog.createLogFileUptoYesterday(cal.getTime());
		
		
		/** Creates log file for printouts taken yesterday. */
		//pLog.createLogFileYesterday();
		
		/** Use the sample file given for parsing. */
		pLog.ParseCSVAndLoadToDB("9Aug2012.csv");
		pLog.cleanUp();
	}
}
