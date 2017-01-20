package com.db.dataManger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Initialize {
	
	//Create Scheduler Logger
	protected static Logger slogger=Logger.getLogger("Scheduler");
	
	//Create Script Loggers as needed
	private static Logger dlogger=Logger.getLogger("Phase1");
	protected static ArrayList<Logger> dmlogger=new ArrayList<Logger>();
		
	protected static ArrayList<Integer> blockList=new ArrayList<Integer>();
	protected static ArrayList<String> blockQueryList=new ArrayList<String>();
	protected static ArrayList<String> scriptNames = new ArrayList<String>();
	protected static ArrayList<String> fileNames = new ArrayList<String>();
	
	public static void main(String args[]) throws IOException {

		long t1 = System.currentTimeMillis();
		long time_taken = 0;
		
		boolean size_accepted = true;
		Scanner sc = new Scanner(System.in);

		// Get and Set the size of the Memory Buffer Made Available to the TRC
		// System
		while (size_accepted) {
			System.out
					.println("\nEnter the Size of the Memory Buffer in multiples of 512(>=3072): ");
			DataManager.data_buffer = sc.nextInt();

			if (DataManager.data_buffer < 3072) {
				System.out.println("\nEnter value >=3072");
				continue;
			}

			if ((DataManager.data_buffer / 512) % 2 != 0) {
				// If odd convert to even for storage
				int odd_val = DataManager.data_buffer / 512;
				odd_val = odd_val - 1;
				DataManager.data_buffer = odd_val * 512;

				// Should not go below minimum
				if (DataManager.data_buffer < 3072) {
					continue;
				}

				System.out.println("The Memory buffer size has been reset to "
						+ DataManager.data_buffer);
				size_accepted = false;
			} else {
				size_accepted = false;
			}

		}

		// Divide the database memory buffer into two equal parts for row and
		// column
		DataManager.row_buff = ByteBuffer
				.allocateDirect(DataManager.data_buffer / 2);
		DataManager.col_buff = ByteBuffer
				.allocateDirect(DataManager.data_buffer / 2);

		// Create Page Tables for row and column
		int num_pages = (DataManager.data_buffer / 2) / 512;

		DataManager.pg_row = new DataManager.PageTable[num_pages - 1];
		for (int i = 0; i < DataManager.pg_row.length; i++) {
			DataManager.pg_row[i] = new DataManager.PageTable();
		}

		DataManager.pg_col = new DataManager.PageTable[num_pages];
		for (int i = 0; i < DataManager.pg_col.length; i++) {
			DataManager.pg_col[i] = new DataManager.PageTable();
		}

		// Number of files to be converted to column store
		int num_of_files;
		System.out
				.println("Enter the number of files to be converted into column store:");
		num_of_files = sc.nextInt();

		// Create MetaData Holders
		DataManager.metaData = new DataManager.MetaData[num_of_files];
		for (int i = 0; i < DataManager.metaData.length; i++) {
			DataManager.metaData[i] = new DataManager.MetaData();
		}

		// Create The Directed Files for the Tables Mentioned
		for (int i = 0; i < num_of_files; i++) {
			String file = "";
			// set metaData for table
			DataManager.metaData[i].info = "".concat(file).concat(
					";id clientname phone;4 16 12");
			// Get the table names from the user
			System.out
					.println("Enter the file to be converted to column store");
			file = sc.next();
			fileNames.add(file);
			file = file.concat(".txt");
			boolean flag_file_exists = true;
			BufferedReader brFile = null;
			try {
				brFile = new BufferedReader(new FileReader(file));
			} catch (FileNotFoundException e) {
				flag_file_exists = false;
			}
			// If file are found store them as column store (In disk according
			// to the Project Phase 1)
			if (flag_file_exists) {
				DataManager.convert_to_columnstore(file, brFile);
				brFile.close();
				System.out.println("Converted to column store");
			}
		}

		// Open The Scripts Needed and Run the Scripts
		System.out.println("Enter s to run scripts or q to quit: ");
		String input = sc.next();

		// Creating Array List to store Script File Names (Check Global ArrayList Creation scriptNames)

		while (!(input.equals("q") || input.equals("Q"))) {
			if (input.equals("s") || input.equals("S")) {
				
				//Create Schedule Logger
				//check if Log File Exists
				String logname="ScheduleLog.log";
				//Create Log File
				FileHandler fh = new FileHandler(logname);  
				slogger.addHandler(fh);
				SimpleFormatter formatter = new SimpleFormatter();  
				fh.setFormatter(formatter);  
				slogger.setUseParentHandlers(false);
				
				// Select Number of Scripts
				System.out.println("Enter number of scripts to run: ");
				int num_scripts = sc.nextInt();

				// Enter Script Names
				int sError = 0;
				for (int iter = 0; iter < num_scripts; iter++) {
					System.out
							.println("Enter FileName(No '.txt' needed) of scirpt "
									+ (iter + 1) + ":");
					String name = sc.next();
					File checkFile = new File(name + ".txt");
					if (checkFile.exists()) {
						scriptNames.add(name);
					} else {
						sError++;
						if (sError == 3) {
							break;
						}
						iter--;
					}
				}

				if (sError == 3) {
					System.out
							.println("FileName entered incorrectly too many times...Exiting");
					break;
				} else {
					sError = 0;
				}

				//Create Loggers for all Scripts
				dmlogger.clear();
				//check if Log File Exists
				for(int iter=0;iter<scriptNames.size();iter++){
					String lname=scriptNames.get(iter).concat(".log");
					//Create Log File
					fh = new FileHandler(lname);  
					dlogger.addHandler(fh);
					formatter = new SimpleFormatter();  
					fh.setFormatter(formatter);  
					slogger.setUseParentHandlers(false);
					dmlogger.add(dlogger);
				}
				
				
				// Select Concurrency Method
				int concurrencyType = 0;
				long seed = 0;
				boolean robin = false;
				boolean random = false;
				while (concurrencyType != 1 && concurrencyType != 2) {
					System.out
							.println("Select Concurrency Method: (By entering index)\n1. Round Robin\n2. Random");
					concurrencyType = sc.nextInt();

					if (concurrencyType == 1) {
						robin = true;
					} else if (concurrencyType == 2) {
						System.out.println("Enter the Seed Value for Ramdon Number Generation:");
						seed=sc.nextLong();
						random = true;
					}
				}

				// Call the Script Concurrency method and Begin Executing
				if (robin == true) {
					System.out.println("Starting.....");
					roundRobin(scriptNames);
					
					//statistics
					long t2 = System.currentTimeMillis();
					time_taken = t2-t1;
					System.out.println("Time taken ="+time_taken);
					System.out.println("Number of reads processed: "+DataManager.stats_read);
					System.out.println("Number of writes processed: "+DataManager.stats_write);
					System.out.println("Average response time: "+(time_taken/(DataManager.stats_write+DataManager.stats_read))+"ms");
					
					robin = false;
				}
				if (random == true) {
					System.out.println("Starting.....");
					randomSelect(scriptNames,seed);
					
					//statistics
					long t2 = System.currentTimeMillis();
					time_taken = t2-t1;
					System.out.println("Time taken ="+time_taken);
					System.out.println("Number of reads processed: "+DataManager.stats_read);
					System.out.println("Number of writes processed: "+DataManager.stats_write);
					System.out.println("Average response time: "+(DataManager.stats_write+DataManager.stats_read)/time_taken);
					
					random = false;
				}

			}

			//Format the Logs
			//Put COMPLETED in all Log Files
			for(int iter=0;iter<scriptNames.size();iter++){
				String logname=scriptNames.get(iter).concat("LogFile.log");
				
				//Formatting Log File
				File filer=new File(logname);
				BufferedReader br=new BufferedReader(new FileReader(filer));
			
				String tempname=scriptNames.get(iter).concat("_LogFile.txt");
				File filew=new File(tempname);
				BufferedWriter bw=new BufferedWriter(new FileWriter(filew));
			
				String str=br.readLine();
				while(str!=null){
					if(str.contains("INFO")){
						bw.write(str);
						bw.newLine();
					}
					str=br.readLine();
				}
			
				bw.write("COMPLETED.\n");	
				
				br.close();
				bw.close();
				filer.deleteOnExit();
			}
			
			// Reset ArrayList that Stores Script FileNames
			scriptNames.clear();

			// Open The Scripts Needed and Run the Scripts
			System.out.println("Enter s to run scripts or q to quit: ");
			input = sc.next();
		}
		sc.close();

	}

	private static void roundRobin(ArrayList<String> sNames) throws IOException {
		
		//Maintain File BLockers in case of Conflicts to Stop Reading from that File
			/* -1 --> Recent Transition from Blocked to Unblocked
			 * 0  --> Unblocked
			 * 1 --> Blocked
			 */
			blockList.clear();
		//Initialize
		for(int iter=0;iter<sNames.size();iter++){
			blockList.add(0);
		}
		//Maintain the Last Query of the Corresponding Blocked File
			blockQueryList.clear();
		//Initialize
		for(int iter=0;iter<sNames.size();iter++){
			blockQueryList.add("");
		}

		//Open file pointer to all the Required Script Files
		ArrayList<BufferedReader> fpList=new ArrayList<BufferedReader>();
		
		for(int iter=0;iter<sNames.size();iter++){
			fpList.add(new BufferedReader(new FileReader(new File(sNames.get(iter) + ".txt"))));
		}
		
		//Read in a round robin fashion
		while (!sNames.isEmpty()) {
			for (int iter = 0; iter < sNames.size(); iter++) {
				
				if(blockList.get(iter)==0){
					String query = fpList.get(iter).readLine();
					if (query != null) {
						System.out.println(query);
						Scheduler.schedule(query,sNames.get(iter));
					} else {
						sNames.remove(iter);
						fpList.remove(iter);
						blockList.remove(iter);
						blockQueryList.remove(iter);
					}
				}else if(blockList.get(iter)==-1){
					Scheduler.schedule(blockQueryList.get(iter),sNames.get(iter));
					blockQueryList.set(iter, "");
					blockList.set(iter, 0);
				}
			}
		}
		

	}

	private static void randomSelect(ArrayList<String> sNames, long seed) throws IOException {

	//Maintain File BLockers in case of Conflicts to Stop Reading from that File
		/* -1 --> Recent Transition from Blocked to Unblocked
		 * 0  --> Unblocked
		 * 1 --> Blocked
		 */
	//Initialize
		blockList.clear();
	for(int iter=0;iter<sNames.size();iter++){
		blockList.add(0);
	}
	//Maintain the Last Query of the Corresponding Blocked File
		blockQueryList.clear();
	//Initialize
	for(int iter=0;iter<sNames.size();iter++){
		blockQueryList.add("");
	}
	
	//Open file pointer to all the Required Script Files
	ArrayList<BufferedReader> fpList=new ArrayList<BufferedReader>();
				
	for(int iter=0;iter<sNames.size();iter++){
		fpList.add(new BufferedReader(new FileReader(new File(sNames.get(iter) + ".txt"))));
	}
		
	while (!sNames.isEmpty()) {
			//Select the File Randomly
			Random random=new Random(seed);
			int rgen=(random.nextInt(sNames.size()));
			
			//Check if Blocked
			if(blockList.get(rgen)==1){
				continue;
			}else if(blockList.get(rgen)==-1){	//Recently Unblocked
				Scheduler.schedule(blockQueryList.get(rgen),sNames.get(rgen));
				blockQueryList.set(rgen, "");
				blockList.set(rgen, 0);
			}
			
			//Now Randomly Generate the Number of lines to read from the selected file
			int readGen=Math.abs(random.nextInt());
			
			for(int r=0;r<readGen;r++){
				String query = fpList.get(rgen).readLine();
				if (query != null) {
					Scheduler.schedule(query,sNames.get(rgen));
				} else {
					sNames.remove(rgen);
					fpList.remove(rgen);
					blockList.remove(rgen);
					blockQueryList.remove(rgen);
					break;
				}
			}
		}
		
	}

}
