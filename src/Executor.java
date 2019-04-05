
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Value;
import org.python.jline.internal.InputStreamReader;
import com.jcraft.jsch.*;

public class Executor implements AutoCloseable {

	private Driver driver;

	public void connect(String uri, String user, String password) {
		driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
	}

	@Override
	public void close() throws Exception {
		driver.close();
	}



	public Map<String, String> getExecuteCommand() {

		Map<String, String> returnMap = new HashMap<>();

		try (Session session = driver.session()) {

			session.writeTransaction(new TransactionWork<String>() {

				@Override
				public String execute(Transaction tx) {

					String executeMergeUser = "MATCH (user:CMDUser{id:" + "9823911005" + "}) RETURN user.cmd, user.fileName, user.datatype , user.operation";
					System.out.println(executeMergeUser);

					StatementResult result = tx.run(executeMergeUser);
					Record record = result.next();
					System.out.println(record.toString());
					String command = "";
					List<Value> vals = record.values();
					
					Value val = vals.get(0);
					System.out.println(val);
					command = val.asString();
					System.out.println(command);
					
					val = vals.get(1);
					System.out.println(val);
					String fName = val.asString();
					System.out.println(fName);
					
					val = vals.get(2);
					System.out.println(val);
					String datatype = val.asString();
					System.out.println(datatype);
					
					val = vals.get(3);
					System.out.println(val);
					String operation = val.asString();
					System.out.println(operation);

					/*
					 * while (result.hasNext()) { System.out.println(counter); record =
					 * result.next(); counter = counter ++ ; System.out.println(record.toString());
					 * command = record.toString();
					 * 
					 * }
					 */

					System.out.println(command);
					returnMap.put("command", command);
					
/*					int indStart = command.indexOf('t');
					int indEnd = command.indexOf('-');

					System.out.println(indStart);
					System.out.println(indEnd);

					String fileName = command.substring(15, indEnd);
					fileName = fileName.trim();
					fileName = fileName + ".dat";
					System.out.println(fileName);*/
					
					
					returnMap.put("fileName", fName.trim());
					returnMap.put("datatype", datatype.trim());
					returnMap.put("operation", operation.trim());
					
					return executeMergeUser;

				}
			});
		}

		return returnMap;
	}
	
	
	public static String getCurrentDir() {
		String executionPath = "";
	    try{
	      executionPath = System.getProperty("user.dir");
	      System.out.print("Executing at =>"+executionPath.replace("\\", "/"));
	    }catch (Exception e){
	      System.out.println("Exception caught ="+e.getMessage());
	    }
	    return executionPath;
	  }
	
	public static void exeTwecollcommand(String command) {
		
		String s = null ;
		command = "python " + command ;
		
		try {
		Process p = Runtime.getRuntime().exec(command.trim());

		BufferedReader in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
		
			while((s = in.readLine()) != null) {
				System.out.println(s);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public void generateUsers( String screen_name) {
		
		try (Session session = driver.session()) {
			
			String greeting = session.writeTransaction(new TransactionWork<String>() {
				
				@Override
				public String execute(Transaction tx) {
					

					
					String executeMergeUser = "MERGE (user:User{screen_name:"+ "\'" + screen_name + "\'}) RETURN user";
					System.out.println(executeMergeUser);
					
					
					StatementResult result = tx.run(executeMergeUser);
				
					
					

					System.out.println("File Exists");

					String createFollower = "LOAD CSV WITH HEADERS FROM \"file:///" + screen_name + ".dat \"AS row \n" + 
					"MATCH (usr:User{screen_name:\'" + screen_name + "\'})\n" + 
					"MERGE(user:User{screen_name:row.screen_name})\n" +
					"ON CREATE SET\n" +
					"user.userid = row.userid,\n" +
					"user.name = coalesce(row.name,\"no_name\"),\n"+
					"user.description = replace(coalesce(row.description,\"_\")," +  "'\"','?'),\n" +
					"user.contributors_enabled = row.contributors_enabled,\n" +
					"user.protected = row.protected,\n" +
					"user.followers = row.followers,\n" +
					"user.following_cnt = row.following_cnt,\n" +
					"user.listed_cnt = row.listed_cnt,\n" +
					"user.status_cnt = row.status_cnt,\n" +
					"user.favourites_cnt = row.favourites_cnt,\n" +
					"user.language = row.language,\n" +
					"user.verified = row.verified,\n" +
					"user.url = coalesce(row.url,\"no_url\"),\n" +
					"user.users_twt_date = row.users_twt_date,\n" +
					"user.location = coalesce(row.location,\"no_location\")\n" +
					"CREATE UNIQUE (user)-[:FOLLOWS]->(usr)\n"+
					"RETURN user.screen_name";
					
					System.out.println(createFollower);
					result = tx.run(createFollower);
					System.out.println("#### FOLLOWERS ADDED");

					return "\n\n#### DONE ####";
				}
			});
			System.out.println(greeting);
		}
	}
	
	public static Map<String,String> readConfig(){
		
		BufferedReader reader ;
		Map<String,String> mp = new HashMap<>();
		try {
			reader = new BufferedReader(new FileReader("config.ini"));
			String line = reader.readLine();
			String[] words = line.split(":", 2);
			System.out.println(words[0]);
			System.out.println(words[1]);
			mp.put(words[0], words[1]);
			
			while(true) {
				
				line = reader.readLine();
				if(line != null && !(line.trim().equals(""))) {
					words = line.split(":", 2);
					System.out.println(words[0]);
					System.out.println(words[1]);
					mp.put(words[0], words[1]);
				}
				else break;
			}
			reader.close();
		}catch(Exception e) {
			e.printStackTrace();
		}
		
		return mp;
		
	}

	public static void main(String[] args) {

		try {

			Map<String,String> configMap = readConfig();
			String neo4jUser = configMap.get("neo4j_user");
			String neo4jPwd = configMap.get("neo4j_pwd");
			
			Executor exe = new Executor();
			exe.connect("bolt://35.244.50.235:7687", neo4jUser.trim(), neo4jPwd.trim());
			Map<String,String> mp = exe.getExecuteCommand();
			exe.close();
			
			String command = mp.get("command");
			String fileName = mp.get("fileName");
			
			
			
			String currDir = getCurrentDir();
			System.out.println("\nCurrent DIR is " + currDir);
			

			
	
			System.out.println("Start running twecoll command ....." + command);
			exeTwecollcommand(command);
			System.out.println("End running Twecoll .....");


			JSch jsch = new JSch();
			com.jcraft.jsch.Session session = null;

//			String user = "divinebhartiyalist";
			String user = configMap.get("ssh_user").trim();
			String host = "35.244.50.235";
//			String ppkKey = "divinebhartiyalist.ppk";
			String ppkKey = configMap.get("ppk_key").trim();

			session = jsch.getSession(user, host);
			// use private key instead of username/password
			session.setConfig("PreferredAuthentications", "publickey,gssapi-with-mic,keyboard-interactive,password");
			jsch.addIdentity(ppkKey);
			java.util.Properties config = new java.util.Properties();
			config.put("StrictHostKeyChecking", "no");
			session.setConfig(config);
			session.connect();

			System.out.println("Connected");

			Channel channel = session.openChannel("sftp");
			channel.setInputStream(System.in);
			channel.setOutputStream(System.out);
			channel.connect();
			System.out.println("shell channel connected....");
			ChannelSftp c = (ChannelSftp) channel;
//			c.put("D:\\NeoData\\followers\\ads883.dat", "/var/lib/neo4j/import");
			c.put("contra_investor.dat", "/var/lib/neo4j/import");
			c.exit();
			System.out.println("done");
			
			

			exe.connect("bolt://35.244.50.235:7687", "neo4j", "######1Aa");
			exe.generateUsers("contra_investor");
			exe.close();
			
			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}