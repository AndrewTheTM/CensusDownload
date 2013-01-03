package org.oki.transmodel.censusDownloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;




public class GetCensus {
	public static Properties jConfig;	
	static Connection theConn;
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		jConfig=new Properties();
		getSetup("CensusDownloader.properties");
		JSONArray censusBG[] = new JSONArray[cInt(jConfig.getProperty("DataLists"))]; 
		JSONArray censusT[]=new JSONArray[cInt(jConfig.getProperty("DataLists"))];
		try{
			theConn=MyConnection.getConnection();
			ResultSet rs;
			Statement stmt;
			String sql;
			sql="SELECT "+jConfig.getProperty("StateField")+", "+jConfig.getProperty("CountyField")+", "+jConfig.getProperty("TractField")+" FROM "+jConfig.getProperty("InputTableName");
			stmt=theConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
			rs=stmt.executeQuery(sql);
			rs.last();
			int rows=rs.getRow();
			rs.first();
			for(int dl=0;dl<cInt(jConfig.getProperty("DataLists"));dl++){
				System.out.println("Getting Data item "+(dl+1)+" of "+jConfig.getProperty("DataLists"));
				String dataListToDo="DataList"+(dl+1);
				int pct=0;
				while(rs.next()){
					pct=Math.round((rs.getRow()/rows)*10);
					if(pct%10==0 && pct>0)
						System.out.print(pct+"%...");
					String state=rs.getString(jConfig.getProperty("StateField"));
					String county=rs.getString(jConfig.getProperty("CountyField"));
					String tract=rs.getString(jConfig.getProperty("TractField"));
									
					String censusURL="http://thedataweb.rm.census.gov/data/2010/"+jConfig.getProperty("DataSource")+"?key="+jConfig.getProperty("CensusAPIKey")+"&get="+jConfig.getProperty(dataListToDo)+"&for=block+group:*&in=state:"+state+"+county:"+county+"+tract:"+tract;
					
					JSONArray json=readJsonFromUrl(censusURL);
					if(censusBG[dl]==null){
						censusBG[dl]=json;
					}else{
						for(int i=1;i<json.size();i++){
							censusBG[dl].add(json.get(i));
						}
					}
					
					censusURL="http://thedataweb.rm.census.gov/data/2010/"+jConfig.getProperty("DataSource")+"?key="+jConfig.getProperty("CensusAPIKey")+"&get="+jConfig.getProperty(dataListToDo)+"&for=tract:*&in=state:"+state+"+county:"+county;
					JSONArray jsont=readJsonFromUrl(censusURL);
					if(censusT[dl]==null){
						censusT[dl]=jsont;
					}else{
						for(int i=1;i<json.size();i++){
							censusT[dl].add(json.get(i));
						}
					}	
				}
				rs.first();
			}
			//Blockgroup Table
			
			//TODO: Create SQLString to create table
			
			//TODO: Iterate through JSON Array
			
			//TODO: Output Table
			
			//TODO: Rinse, lather, repeat
			
			//TODO: Insert coin and play again
			
			
		}catch(Exception e){
			e.printStackTrace();
		}
		int a=1;
		System.out.println(a);
	}

	/**
	 * Loads the jConfig variable with items from the setup file
	 * @param filename The properties filename
	 * @throws IOException
	 */
	public static void getSetup(String filename) throws IOException{
		ClassLoader loader=Thread.currentThread().getContextClassLoader();
		InputStream stream=loader.getResourceAsStream(filename);
		jConfig.load(stream);
		// Get InputTable datapath and add to jConfig
		if(jConfig.getProperty("InputTable", null) != null){
			String dpath=jConfig.getProperty("InputTable").substring(0, jConfig.getProperty("InputTable").lastIndexOf("\\"));
			jConfig.put("InputDataPath", dpath);
			String tname=jConfig.getProperty("InputTable").substring(jConfig.getProperty("InputTable").lastIndexOf("\\")+1,jConfig.getProperty("InputTable").lastIndexOf("."));
			jConfig.put("InputTableName",tname);
		}
	}

	public static JSONArray readJsonFromUrl(String url) throws IOException {
		InputStream is = new URL(url).openStream();
		try {
			BufferedReader rd = new BufferedReader(new InputStreamReader(is, Charset.forName("UTF-8")));
		    String jsonText = readAll(rd);
		    Object obj=JSONValue.parse(jsonText);
		    JSONArray array=(JSONArray)obj;
		    return array;
		} finally {
		    is.close();
		}
	}
	
	private static String readAll(Reader rd) throws IOException {
	    StringBuilder sb = new StringBuilder();
	    int cp;
	    while ((cp = rd.read()) != -1) {
	      sb.append((char) cp);
	    }
	    return sb.toString();
	  }
	
	private static int cInt(String input){
		Integer inputInteger=Integer.parseInt(input);
		return inputInteger.intValue();
	}
}

/**
 * Class for database connection
 * @author arohne
 *
 */
class MyConnection{
	public static Connection getConnection() throws Exception {
	    Driver d = (Driver)Class.forName
	     ("sun.jdbc.odbc.JdbcOdbcDriver").newInstance();
	    Connection c = DriverManager.getConnection(
	     "jdbc:odbc:Driver={Microsoft dBASE Driver (*.dbf)}; DriverID=277;DBQ="+GetCensus.jConfig.getProperty("InputDataPath")
	      );
	    return c;  
	    }
}




