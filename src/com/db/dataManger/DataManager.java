package com.db.dataManger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.logging.Logger;

public class DataManager {
	
		//Logger
		private static Logger logger;
		
		//for calculating statistics
		protected static int stats_read = 0;
		protected static int stats_write = 0;
	
		protected static int data_buffer;
		protected static ByteBuffer row_buff, col_buff;
		protected static PageTable[] pg_row, pg_col;
		protected static MetaData[] metaData;
		protected static int read_counter=0;
		protected static boolean dup_flag=false;
		
		public static void runQuery(String query, String script) throws IOException{
					
			//Get Logger for Writing into Log File for that script
			for(int iter=0;iter<Initialize.scriptNames.size();iter++){
				if(Initialize.scriptNames.get(iter).contentEquals(script)){
					logger=Initialize.dmlogger.get(iter);
				}
			}
					
			//Initialize variables
			String type,table,value="";
	
			//Running Query
			logger.info(query+"\n");
						
			//Reset read_counter=0
			read_counter=0;
					
			//Split the Query
			String[] query_parts=query.split(" ");
				
			type=query_parts[0];
			table=query_parts[1];
			if(query_parts.length==3){
				value=query_parts[2];
			}
						
			//checking if file exists or not
			String file_name=table.concat(".txt");
			boolean flag_file_exists = false;
			for(int iter=0;iter<Initialize.fileNames.size();iter++){
				if(table.contentEquals(Initialize.fileNames.get(iter))){
					flag_file_exists=true;
				}
			}
			if(!flag_file_exists){
				return;
			}
				
			BufferedReader brFile;
			try{
				brFile=new BufferedReader(new FileReader(file_name));
				brFile.close();
			}catch(Exception e){
				flag_file_exists=false;
			}
			
						//Check If Row Query
						if(type.contentEquals("R")||type.contentEquals("I")||type.contentEquals("D")){
							
							//Check if table exists, **Create if Insert Query
							
							if(!flag_file_exists && type.contentEquals("R")){
								//Aborted Log Error
								logger.info("Query Aborted: Table does no Exist.\n");
								
								return;
							}else if(type.contentEquals("R")){
								if(value.contentEquals("")){
									//Aborted Log Error
									logger.info("Query Aborted:  Read Query Without Value.\n");
									
									return;
								}
								read(table,value);
								stats_read++;
								return;
							}
								
							if(!flag_file_exists && type.contentEquals("I")){
								value=value.substring(1, value.length()-1);
								File file=new File(file_name);
								BufferedWriter bw=new BufferedWriter(new FileWriter(file));
								bw.write(value);
								bw.close();
								brFile=new BufferedReader(new FileReader(file));
								convert_to_columnstore(file.getName(),brFile);
								logger.info("Table created and record "+table+", "+type+", "+value+" inserted");
								brFile.close();
								flag_file_exists=true;
						
								return;
									
							}else if(type.contentEquals("I")){
								value=value.substring(1, value.length()-1);
								if(value.split(",").length!=3){
									//Aborted Log Error
									logger.info("Query Aborted: Invalid Insertion.\n");
								
									return;
								}
								insert(table,value);
								
								stats_write++;
								
								return;
							}
							
							if(!flag_file_exists && type.contentEquals("D")){
								//Aborted Log Error
								logger.info("Query Aborted: Table doesn't Exist.\n");
								
								return;
							}else if(type.contentEquals("D")){
								flag_file_exists=delete(table);
	
								return;
							}else{
								//Aborted Log Error
								logger.info("Query Aborted: Invalid Query\n");
								
								return;
							}
						}
						
						//Check If Column Query
						if(type.contentEquals("M")||type.contentEquals("G")){
							if(!flag_file_exists && type.contentEquals("M")){
								//table doesn't exist error
								logger.info("Query Aborted: Table doesn't exist");
							}
							else if(type.contentEquals("M")){
								if(value.contentEquals("")){
									//Aborted Log Error
									logger.info("Query Aborted: Area code not provided");
									
									return;
								}
								
								//Find all dirty Pages
								for(int iter=0;iter<pg_row.length;iter++){
									if(pg_row[iter].dirty_bit==1){
										//Flush the dirty Page
										//For id file
										Updater(pg_row[iter].table.concat("_id.txt"), 4, pg_row[iter].hash_value, pg_row[iter].offset, iter);
										//For name file
										Updater(pg_row[iter].table.concat("_phone.txt"), 12, pg_row[iter].hash_value, pg_row[iter].offset, iter);
										//For phone file
										Updater(pg_row[iter].table.concat("_clientname.txt"), 16, pg_row[iter].hash_value, pg_row[iter].offset, iter);
										
										//Reset Page Table
										pg_row[iter]=new PageTable();
									}
								}
								
								//perform the query
								retrieve_records(table, value);
								stats_read++;
							}
							if(!flag_file_exists && type.contentEquals("G")){
								//table doesn't exist error
								logger.info("Query Aborted: Table doesn't exist");
							}
							else if(type.contentEquals("G")){
								if(value.contentEquals("")){
									//Aborted Log Error
									logger.info("Query Aborted: Area code not provided");
									
									return;
								}
								//Find all dirty Pages
								for(int iter=0;iter<pg_row.length;iter++){
									if(pg_row[iter].dirty_bit==1){
										//Flush the dirty Page
										//For id file
										Updater(pg_row[iter].table.concat("_id.txt"), 4, pg_row[iter].hash_value, pg_row[iter].offset, iter);
										//For name file
										Updater(pg_row[iter].table.concat("_phone.txt"), 12, pg_row[iter].hash_value, pg_row[iter].offset, iter);
										//For phone file
										Updater(pg_row[iter].table.concat("_clientname.txt"), 16, pg_row[iter].hash_value, pg_row[iter].offset, iter);
										
										//Reset Page Table
										pg_row[iter]=new PageTable();
										
									}
								}
								//perform the query
								read_phone(table,value);
								stats_read++;
							}
						}	
			
		}
		
		private static void read_phone(String table, String value) throws IOException{
			
			String filename = table.concat("_phone").concat(".txt");
			//getting the page
			ByteBuffer phone_buffer = ByteBuffer.allocateDirect(512);
			
			//gives the number of customers with the area code
			int count_of_phones = 0;
			int pageTable_size = pg_col.length;
			int[] empty_positions = new int[pageTable_size];
			int empty_slots = 0;
			boolean empty = false;
			//for finding the empty pages
			for(int i=0;i<pageTable_size;i++){
				if(pg_col[i].hash_value==-1){
					empty_slots=empty_slots+1;
					empty_positions[i]=1;
					continue;
				}
			}
			
			//opening the hashed phone file
			File file=new File(filename);
			RandomAccessFile raf=new RandomAccessFile(file, "r");
			
			//int start_location = 64;
			//pages_to_be_read
			long pages_to_read;
			if( ((raf.length()-64) % 512) >0 )
			{
				//check
				//pages_to_read =  ((raf.length()-64) / 512)+1;
				pages_to_read =  ((raf.length()-64) / 512);
			}
			else
			{
				pages_to_read = ((raf.length()-64) / 512);
			}
			
			
			//for calculating page #s and hash values
			//hash_value contains the current page's hash_value
			//page number contains the current page's page number corresponding to the hash value
			int[] hash_value = new int[(int)pages_to_read];
			int[] page_number = new int[(int)pages_to_read];
			
			//page_count gives the current page number considering all the pages
			int page_count = 0;
			int pointer_value;
			//present_page gives the current page number considering the pages for the hash_value
			int present_page;
			for(int i=0;i<16;i++)
			{
				present_page=0;
				raf.seek(64+(i*512)+508);
				pointer_value = raf.readInt();
				hash_value[i] = i;
				page_number[i] = present_page;
				page_count++;
				present_page++;
				while(pointer_value!=-1)
				{
					hash_value[pointer_value] = i;
					page_number[pointer_value] = present_page;		
					
					raf.seek(64+(pointer_value*512)+508);
					pointer_value = raf.readInt();
					page_count++;
					present_page++;
				}
			}
			
			
			
			int pages_read = 0;
			int i;
			int page_to_swap;
			while(pages_to_read!=pages_read)
			{
				//finding whether there is an empty page
				phone_buffer.clear();
				for(page_to_swap=0;page_to_swap<pageTable_size;page_to_swap++)
				{
					empty = false;
					if(empty_positions[page_to_swap]==1)
					{
						empty = true;
						empty_positions[page_to_swap] = 0;
						break;
					}
				}

				phone_buffer = get_phone_page(raf, pages_read);
				
				
				pages_read++;
				//writing into column buffer
				//check for an empty page in the buffer 
				if(empty)
				{
					//LOG SWAP IN
					logger.info("SWAP IN T-"+table+" P-"+page_number[pages_read-1]+" B-"+hash_value[pages_read-1]+"\n");
					
					for(i =0;i<512;i++)
					{
						col_buff.put( ((page_to_swap*512)+i),(phone_buffer.get(i)));
					}
					//pg_col[k].dirty_bit=1;
					pg_col[page_to_swap].time=System.currentTimeMillis();
					pg_col[page_to_swap].hash_value = hash_value[pages_read-1]; 
					pg_col[page_to_swap].table = table;
					pg_col[page_to_swap].dirty_bit=0;
					pg_col[page_to_swap].offset=page_number[pages_read-1]*512;
				}
				else//swap existing page
				{
					page_to_swap = 0;
					for(i=0;(i+1)<pageTable_size;i++)
					{
						if(pg_col[page_to_swap].time<pg_col[i+1].time)
						{
							page_to_swap = i;
						}
						else
						{
							page_to_swap = i+1;
						}
					}

					//LOG SWAP OUT
					logger.info("SWAP OUT T-"+pg_col[page_to_swap].table+" P-"+pg_col[page_to_swap].offset/512+" B-"+pg_col[page_to_swap].hash_value+"\n");

					
					for(i =0;i<512;i++)
					{
						col_buff.put( (page_to_swap*512)+i,(phone_buffer.get(i)));
					}
					pg_col[page_to_swap] = new PageTable();
					pg_col[page_to_swap].time=System.currentTimeMillis();
					pg_col[page_to_swap].hash_value = hash_value[pages_read-1]; 
					pg_col[page_to_swap].table = table;
					pg_col[page_to_swap].dirty_bit=0;
					pg_col[page_to_swap].offset=page_number[pages_read-1];
					//LOG SWAP IN
					logger.info("SWAP IN T-"+pg_col[page_to_swap].table+" P-"+pg_col[page_to_swap].offset/512+" B-"+pg_col[page_to_swap].hash_value+"\n");

				}
				
				//compare and count
				String b_str;
				for(i=0;i<42;i++)
				{
					int b = col_buff.getInt((page_to_swap*512)+(12*i)); 
					if(b<100)
					{
						b_str = "0"+String.valueOf(b);
					}
					else
						b_str = String.valueOf(b);
					if(value.equals(String.valueOf(b_str)))
					{
						
						count_of_phones++;
					}
				}
				
			}
			

		
			logger.info("GCount: "+count_of_phones);
			raf.close();
		}
		
		private static ByteBuffer get_phone_page(RandomAccessFile raf, int pages_read) throws IOException
		{
			ByteBuffer phone_page = ByteBuffer.allocateDirect(512);
			int start_location = 64+(pages_read*512);
			raf.seek(start_location);
			for(int i=0;i<512;i++)
			{
				phone_page.put(raf.readByte());
			}
			return phone_page;
		}
		
		private static void retrieve_records(String table, String value) throws IOException{
			
			String att1_file = table.concat("_id").concat(".txt");
			String att2_file = table.concat("_clientname").concat(".txt");
			String att3_file = table.concat("_phone").concat(".txt");
			
			ByteBuffer phone_buffer = ByteBuffer.allocateDirect(512);
			
			//variables
			int pageTable_size = pg_col.length;
			int[] empty_positions = new int[pageTable_size];
			int empty_slots = 0;
			boolean empty = false;
			
			//for finding the empty pages
			for(int i=0;i<pageTable_size;i++){
				if(pg_col[i].hash_value==-1){
					empty_slots=empty_slots+1;
					empty_positions[i]=1;
					continue;
				}
			}
		
			//opening the files
			File id_file=new File(att1_file);
			File name_file=new File(att2_file);
			File phone_file=new File(att3_file);
			RandomAccessFile raf_id=new RandomAccessFile(id_file, "r");
			RandomAccessFile raf_name=new RandomAccessFile(name_file, "r");
			RandomAccessFile raf_phone=new RandomAccessFile(phone_file, "r");
			
			//for storing the results
			ArrayList<String> id_al = new ArrayList<String>();
			ArrayList<String> name_al = new ArrayList<String>();
			ArrayList<String> phone_al = new ArrayList<String>();
			
			int start_location = 64;
			int num_of_hash_values = 16;
			
			//for storing the record parsed in particular hash value  buckets 
			int record_parsed;
			//for storing the record numbers in particular hash value buckets
			ArrayList<String> record_num = new ArrayList<String>();
			
			int page_to_swap;
			int current_bucket, i;
			for(int hash_val=0;hash_val<num_of_hash_values;hash_val++)
			{	
				record_parsed = 0;
				//for reaching the flow pointer
				raf_phone.seek(start_location+(hash_val*512)+508);
				record_num.clear();
				//for the present bucket
				current_bucket = hash_val;
		
				//for page_number count corresponding to the hash value
				int page_number=0;
				do
				{
					
					//finding whether there is an empty page
					phone_buffer.clear();
					empty = false;
					for(page_to_swap=0;page_to_swap<pageTable_size;page_to_swap++)
					{
						if(empty_positions[page_to_swap]==1)
						{
							empty = true;
							empty_positions[page_to_swap] = 0;
							break;
						}
						
					}
					
					for(int j=0;j<512;j++)
					{
						raf_phone.seek(start_location+ (current_bucket*512)+j );
						phone_buffer.put(j, raf_phone.readByte());
					}
					
					//if there is an empty page, place the buffered page in that position
					if(empty)
					{
						for(i =0;i<512;i++)
						{
							col_buff.put( (page_to_swap*512)+i,(phone_buffer.get(i)));
						}
						//pg_col[page_to_swap].dirty_bit=1; No dirty bits for read
						pg_col[page_to_swap].time=System.currentTimeMillis();
						pg_col[page_to_swap].hash_value = hash_val;
						pg_col[page_to_swap].dirty_bit=0;
						pg_col[page_to_swap].table = table;
						pg_col[page_to_swap].offset = page_number*512;
						
						//LOG SWAP IN
						logger.info("SWAP IN T-"+pg_col[page_to_swap].table+" P-"+pg_col[page_to_swap].offset/512+" B-"+pg_col[page_to_swap].hash_value+"\n");
						
					}
					else//swap existing page
					{
						page_to_swap = 0;
						for(i=0;(i+1)<pageTable_size;i++)
						{
							if(pg_col[page_to_swap].time<pg_col[i+1].time)
							{
								page_to_swap = i;
							}
							else
							{
								page_to_swap = i+1;
							}
						}
						
						//LOG SWAP OUT
						logger.info("SWAP OUT T-"+pg_col[page_to_swap].table+" P-"+pg_col[page_to_swap].offset/512+" B-"+pg_col[page_to_swap].hash_value+"\n");
						
						//move the page into buffer
						for(i =0;i<512;i++)
						{
							col_buff.put( (page_to_swap*512)+i,(phone_buffer.get(i)));
						}
						//pg_col[page_to_swap].dirty_bit=1;
						pg_col[page_to_swap].time=System.currentTimeMillis();
						pg_col[page_to_swap].hash_value = hash_val;
						pg_col[page_to_swap].dirty_bit=0;
						pg_col[page_to_swap].table = table;
						pg_col[page_to_swap].offset = page_number*512;
						
						//LOG SWAP IN
						logger.info("SWAP IN T-"+pg_col[page_to_swap].table+" P-"+pg_col[page_to_swap].offset/512+" B-"+pg_col[page_to_swap].hash_value+"\n");
					}
					
					//compare
					int j;
					int b;
					int c[] = new int[3]; // storing phone number as three ints
					String str_phone="";
					String b_str="";
					for(i=0;i<42;i++)//42 phone numbers
					{
						
						b = col_buff.getInt((page_to_swap*512)+(12*i));
						if(b<100)
						{
							b_str = "0"+String.valueOf(b);
						}
						else
							b_str = String.valueOf(b);
			
						if(value.equals(b_str))
						{
							for(j=0;j<3;j++) //3 ints in phone num
							{
								c[j] = col_buff.getInt((page_to_swap*512)+(i*12)+j*4);
							}
							str_phone = String.valueOf(c[0])+String.valueOf(c[1])+String.valueOf(c[2]) ;
							
							//store the phone value in arraylist "phone_al" and its position value in arraylist "record_num"
							phone_al.add(str_phone);
							//storing integer(record number) as a string
							record_num.add(String.valueOf(record_parsed+i));
							
						}
					}
					
					
					
					
					//check end pointer and move
					raf_phone.seek( 64+(current_bucket*512)+504);
					int end_value = raf_phone.readInt();
					record_parsed = record_parsed + end_value/12;
					raf_phone.seek( 64+(current_bucket*512)+508);
					current_bucket = raf_phone.readInt();
					page_number++;
				}while( current_bucket!=-1);
				
				
				//Retrieve id and name for the corresponding record_numbers of the hash_val
				int jumps_needed, rec_position_in_bucket ;
				int required_bucket = hash_val;
				
				//for id
				for(int loop_id = 0; loop_id<record_num.size(); loop_id++ )
				{
					//Converting object type to int type
					jumps_needed = Integer.parseInt((String) record_num.get(loop_id)) /126;
					//making jumps to reach the required bucket
					while(jumps_needed!=0)
					{
						raf_id.seek(64+(required_bucket*512)+508);
						required_bucket = raf_id.readInt();
						jumps_needed--;
					}
				
					rec_position_in_bucket = Integer.parseInt((String) record_num.get(loop_id)) %126;
					raf_id.seek(64+(required_bucket*512)+(rec_position_in_bucket)*4); //4 for id
					id_al.add(String.valueOf(raf_id.readInt()));
				}
				
				//for name
				required_bucket = hash_val;
				byte[] b_name;
				for(int loop_name = 0; loop_name<record_num.size(); loop_name++ )
				{
					//Converting object type to int type
					jumps_needed = Integer.parseInt((String) record_num.get(loop_name))/31; //31 name records can be stored in name file
					//making jumps to reach the required bucket
					while(jumps_needed!=0)
					{
						raf_name.seek(64+(required_bucket*512)+508);
						required_bucket = raf_name.readInt();
						jumps_needed--;
					}
					rec_position_in_bucket = Integer.parseInt((String) record_num.get(loop_name))%31;
					b_name = new byte[16];
					raf_name.seek(64+(required_bucket*512)+(rec_position_in_bucket)*16);
					for(int m=0;m<16;m++)
					{
						b_name[m] = raf_name.readByte();
					}
					String b = new String(b_name);
					name_al.add(b);
				}
				
			}
			record_num.clear();
			raf_id.close();
			raf_phone.close();
			raf_name.close();
			
			if(phone_al.size()==0)
			{
				logger.info("No match found");
			}
			else
			{
				for(i=0;i<phone_al.size();i++)
				{
					logger.info((String) id_al.get(i)+" , "+name_al.get(i).trim()+" , "+(String) phone_al.get(i));
				}
			}
			
		}
		
		private static boolean delete(String table){
			
			File filename=new File(table.concat(".txt"));
			filename.delete();
			
			File id_filename=new File(table.concat("_").concat("id.txt"));
			id_filename.delete();
			
			File name_filename=new File(table.concat("_").concat("clientname.txt"));
			name_filename.delete();
			
			File phone_filename=new File(table.concat("_").concat("phone.txt"));
			phone_filename.delete();
			
			for(int i=0;i<pg_row.length;i++){
				if(pg_row[i].table.contentEquals(table)){
					pg_row[i]=new PageTable();
				}
			}
			
			for(int i=0;i<pg_col.length;i++){
				if(pg_col[i].table.contentEquals(table)){
					pg_col[i]=new PageTable();
				}
			}
			
			return false;
		}
		
		private static int read(String table, String value) throws IOException{
			
			int recordFound=0;
			
			int id=Integer.parseInt(value);
			
			//Hash The ID to get bucket Location
			int hash_value=id%16;
			
			//Check Row Buffer for existing bucket page using page table
			boolean match=false,empty=false;
			int empty_slots=0;
			int pageTable_size=pg_row.length;
			int[] match_positions = new int[pageTable_size];
			int[] empty_positions = new int[pageTable_size];
			
			for(int i=0;i<pageTable_size;i++){
				if(pg_row[i].hash_value==-1){
					//No entry
					empty=true;
					empty_slots=empty_slots+1;
					empty_positions[i]=1;
					continue;
				}else{
					//check for match
					if(pg_row[i].hash_value==hash_value && pg_row[i].table.contentEquals(table)){
						//Find the match and store in a array
						match=true;
						match_positions[i]=1;
					}else{
						continue;
					}
				}
			}

			//Variable Initializations
			int end,end_value;
			int index = 0;
			boolean read_success=false;
			
			//If a page exists on the buffer with the same hash value
			if (match){
				
				//Check if existing buckets that match are empty to store
				for(int i=0;i<match_positions.length;i++){
					//Check all matching buckets 
					if(match_positions[i]==1){
						
						//Try to find the id, i.e., search the page
						//Get end value
						end=(i*512)+504;
						end_value=row_buff.getInt(end);
						
						for(int j=0;j<end_value/32;j++){
							if(row_buff.getInt((i*512)+(j*32))==id){
								read_success=true;
								index=j;
								break;
							}
						}
						
						//Display the read value if read_success
						if(read_success){
							recordFound=1;
							
							int rid=row_buff.getInt((i*512)+(index*32));
							String r=String.valueOf(rid);
							
							byte[] disp=new byte[16];
							for(int j=0;j<16;j++){
								disp[j]=row_buff.get((i*512)+(index*32)+4+j);
							}
							String n=new String(disp);
							n=n.trim();
							
							String p1="00";
							String p2="00";
							String p3="000";
							
							p1=p1.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+20)));
							p2=p2.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+24)));
							p3=p3.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+28)));
							
							p1=p1.substring(p1.length()-3, p1.length());
							p2=p2.substring(p2.length()-3, p2.length());
							p3=p3.substring(p3.length()-4, p3.length());
							
							//LOG
							logger.info("Read: "+r+", "+n+", "+p1+"-"+p2+"-"+p3+"\n");
							
							pg_row[i].time=System.currentTimeMillis();
							break;
						}
						
					}
				}
			}
			
			//Write Back Any Dirty Pages of the corresponding hash value before beginning the sequence read process
			if(!read_success){
				for(int i=0;i<pg_row.length;i++){
					if(pg_row[i].table.contentEquals(table) && pg_row[i].hash_value==hash_value && pg_row[i].dirty_bit==1){
						//Updates the Write position for the id Attribute File
						String filename=table.concat("_").concat("id.txt");
						Updater(filename,4,pg_row[i].hash_value,pg_row[i].offset,i);
						
						//Updates the Write position for the name Attribute File
						filename=table.concat("_").concat("clientname.txt");
						Updater(filename,16,pg_row[i].hash_value,pg_row[i].offset,i);
						
						//Updates the Write position for the phone Attribute File
						filename=table.concat("_").concat("phone.txt");
						Updater(filename,12,pg_row[i].hash_value,pg_row[i].offset,i);
						
						pg_row[i]=new PageTable();
						
						empty=true;
						empty_positions[i]=1;
					}
				}
			}
			
			boolean skip=false;
			ByteBuffer newPage = null;
			//Check For Empty Slots
			if(empty && !read_success){
				for(int i=0;i<empty_positions.length;i++){
					if(empty_positions[i]==1){
						//Get The Page From Memory To Insert
						newPage=getNext15Records(hash_value,table);
						
						//manage offset
						int offset=read_counter*15*4;
						int adjust=(offset/504)*8;
						offset=offset+adjust;
						
						//Check if page already exists
						for(int k=0;k<pg_row.length;k++){
							if(pg_row[k].table.contentEquals(table) && pg_row[k].hash_value==hash_value && pg_row[k].offset==offset){
								skip=true;
							}
						}
						
						if(skip){
							skip=false;
							continue;
						}
						
						//SET PAGE ENTRIES
						pg_row[i].dirty_bit=0;
						pg_row[i].hash_value=hash_value;
						pg_row[i].offset=offset;
						pg_row[i].table=table;
						pg_row[i].time=System.currentTimeMillis();
						
						//LOG SWAP IN
						logger.info("SWAP IN T-"+pg_row[i].table+" P-"+pg_row[i].offset/512+" B-"+pg_row[i].hash_value+"\n");
						
						//Copy new page into memory buffer (Has to have space to store)
						for(int j=0;j<512;j++){
							row_buff.put((i*512)+j, newPage.get(j));						
						}
						
						//Get end value
						end=(i*512)+504;
						end_value=row_buff.getInt(end);
						
						//Search
						for(int j=0;j<end_value/32;j++){
							if(row_buff.getInt((i*512)+(j*32))==id){
								read_success=true;
								index=j;
								break;
							}
						}
						
						//Display the read value if read_success
						if(read_success){
							recordFound=1;
							
							int rid=row_buff.getInt((i*512)+(index*32));
							String r=String.valueOf(rid);
							
							byte[] disp=new byte[16];
							for(int j=0;j<16;j++){
								disp[j]=row_buff.get((i*512)+(index*32)+4+j);
							}
							String n=new String(disp);
							n=n.trim();
							
							String p1="00";
							String p2="00";
							String p3="000";
							
							p1=p1.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+20)));
							p2=p2.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+24)));
							p3=p3.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+28)));
							
							p1=p1.substring(p1.length()-3, p1.length());
							p2=p2.substring(p2.length()-3, p2.length());
							p3=p3.substring(p3.length()-4, p3.length());
							
							//LOG
							logger.info("Read: "+r+", "+n+", "+p1+"-"+p2+"-"+p3+"\n");
							
							
							pg_row[i].time=System.currentTimeMillis();
							break;
					}else{
						//Checking if last pages
						if(read_counter==-1){
							logger.info("No Record Found");
							read_success=true;
							break;
						}
							
					}
				}
			}
			}
			
			skip=false;
			//No Match Exists or Match Did not help and No Empty Slots are available
			if(!read_success){
				do{
				//Now we swap
				int i=swap_row();
				
				//LOG SWAP OUT
				logger.info("SWAP OUT T-"+pg_row[i].table+" P-"+pg_row[i].offset/512+" B-"+pg_row[i].hash_value+"\n");
				
				//Replace and Insert
				//Get The Page From Memory To Insert
				newPage=getNext15Records(hash_value,table);
				
				//manage offset
				int offset=read_counter*15*4;
				int adjust=(offset/512)*8;
				offset=offset+adjust;
				
				//Check if page already exists
				for(int k=0;k<pg_row.length;k++){
					if(pg_row[k].table.contentEquals(table) && pg_row[k].hash_value==hash_value && pg_row[k].offset==offset){
						skip=true;
					}
				}
				
				if(skip){
					skip=false;
					continue;
				}
				
				//SET PAGE ENTRIES
				pg_row[i].dirty_bit=0;
				pg_row[i].hash_value=hash_value;
				pg_row[i].offset=offset;
				pg_row[i].table=table;
				pg_row[i].time=System.currentTimeMillis();
				
				//Copy new page into memory buffer (Has to have space to store)
				for(int j=0;j<512;j++){
					row_buff.put((i*512)+j, newPage.get(j));						
				}
				
				//LOG SWAP IN
				logger.info("SWAP IN T-"+pg_row[i].table+" P-"+pg_row[i].offset/512+" B-"+pg_row[i].hash_value+"\n");
				
				//Get end value
				end=(i*512)+504;
				end_value=row_buff.getInt(end);
				
				//Search
				for(int j=0;j<end_value/32;j++){
					if(row_buff.getInt((i*512)+(j*32))==id){
						read_success=true;
						index=j;
						break;
					}
				}
				
				//Display the read value if read_success
				if(read_success){
					recordFound=1;
					
					int rid=row_buff.getInt((i*512)+(index*32));
					String r=String.valueOf(rid);
					
					byte[] disp=new byte[16];
					for(int j=0;j<16;j++){
						disp[j]=row_buff.get((i*512)+(index*32)+4+j);
					}
					String n=new String(disp);
					n=n.trim();
					
					String p1="00";
					String p2="00";
					String p3="000";
					
					p1=p1.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+20)));
					p2=p2.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+24)));
					p3=p3.concat(String.valueOf(row_buff.getInt((i*512)+(index*32)+28)));
					
					p1=p1.substring(p1.length()-3, p1.length());
					p2=p2.substring(p2.length()-3, p2.length());
					p3=p3.substring(p3.length()-4, p3.length());
					
					//LOG
					logger.info("Read: "+r+", "+n+", "+p1+"-"+p2+"-"+p3+"\n");
					
					pg_row[i].time=System.currentTimeMillis();
					break;
				}else{
					//Checking if last pages
					if(read_counter==-1){
						logger.info("No Record Found");
						break;
						}
					}
				}while(read_counter!=-1);
			}
			
			read_counter=0;
			return recordFound;
		}
		
		private static void insert(String table,String value) throws IOException{
			
			String[] parts=value.split(",");
			int id=Integer.parseInt(parts[0]);
			String name=parts[1];
			String phone=parts[2];
			
			//Running Check on Name, if length > 16 then abort the insertion of the row and log Error
			byte[] name_check=name.getBytes();
			byte[] name_bytes;
			
			//Check for length constraints
			if(name_check.length>16){
				//Log Errors
				return;
			}else{
				//Name stored in name_check variable as a byte array of length 16 or less
				//Format it to make it 16 always
				name_bytes=new byte[16];
						
				for(int loop=0;loop<name_check.length;loop++){
					name_bytes[loop]=name_check[loop];
				}
				
				String blank=" ";
				for(int loop=name_check.length;loop<16;loop++){
					name_bytes[loop]=blank.toString().getBytes()[0];
				}
			}
			
			String[] phone_parts=phone.split("-");
			int phone1=Integer.parseInt(phone_parts[0]);
			int phone2=Integer.parseInt(phone_parts[1]);
			int phone3=Integer.parseInt(phone_parts[2]);
			
			//Hash The ID to get bucket Location
			int hash_value=id%16;
			
			//Check Row Buffer for existing bucket page using page table
			boolean match=false,empty=false;
			int empty_slots=0;
			int pageTable_size=pg_row.length;
			int[] match_positions = new int[pageTable_size];
			int[] empty_positions = new int[pageTable_size];
			
			for(int i=0;i<pageTable_size;i++){
				if(pg_row[i].hash_value==-1){
					//No entry
					empty=true;
					empty_slots=empty_slots+1;
					empty_positions[i]=1;
					continue;
				}else{
					//check for match
					if(pg_row[i].hash_value==hash_value && pg_row[i].table.contentEquals(table)){
						//Find the match and store in a array
						match=true;
						match_positions[i]=1;
					}else{
						continue;
					}
				}
			}
			
			//Variable Initializations
			boolean storeflag=false;
			int end,end_value;
			
			//If a page exists on the buffer with the same hash value
			if (match){
				
				//Check if existing buckets that match are empty to store
				for(int i=0;i<match_positions.length;i++){
					//Check all matching buckets 
					//**NOTE:(Only the the last overflow page will be empty)
					if(match_positions[i]==1){
						
						//Get end value
						end=(i*512)+504;
						end_value=row_buff.getInt(end);
						if(end_value>=480){
							//check the next match
							continue;
						}
						
						//Check Duplicates
						getLast15Records(hash_value,id,table,i);
						
						//Duplicate Found Check
						if(dup_flag==true){
							//LOG DUPLICATE FOUND
							logger.info("Insert Aborted, PK IC: Duplicate Record Found = "+id);
							dup_flag=false;
							return;
						}
						
						
						//Store and modify the page table entry
							//ID
						row_buff=row_buff.putInt((i*512)+end_value, id);
						end_value=end_value+4;
						row_buff=row_buff.putInt(end, end_value);
						
							//NAME
						for(int loop=0;loop<16;loop++){
							row_buff=row_buff.put((i*512)+end_value+loop, name_bytes[loop]);
						}
						end_value=end_value+16;
						row_buff=row_buff.putInt(end, end_value);
						
							//PHONE
						row_buff=row_buff.putInt((i*512)+end_value, phone1);
						row_buff=row_buff.putInt((i*512)+end_value+4, phone2);
						row_buff=row_buff.putInt((i*512)+end_value+8, phone3);
						end_value=end_value+12;
						row_buff=row_buff.putInt(end, end_value);
						
						//Manage End Pointer if 480
						if(end_value==480){
							end_value=504;
							row_buff.putInt(end, end_value);
						}
						
						//Set Dirty Bit
						pg_row[i].dirty_bit=1;
						pg_row[i].time=System.currentTimeMillis();
						
						//Insertion Completed
						storeflag=true;
						
						//LOG INSERTED
						logger.info("Inserted: "+id+", "+name+", "+phone+"\n");
						
						break;
					}
				}
			}
			
			//Before Inserting we need to write back all Dirty Pages of that Hash Value Back into memory
			//Write into the physical memory
			if(!storeflag){
				for(int i=0;i<pg_row.length;i++){
					if(pg_row[i].table.contentEquals(table) && pg_row[i].hash_value==hash_value && pg_row[i].dirty_bit==1){
						
						//Updates the Write position for the id Attribute File
						String filename=table.concat("_").concat("id.txt");
						Updater(filename,4,pg_row[i].hash_value,pg_row[i].offset,i);
						
						//Updates the Write position for the name Attribute File
						filename=table.concat("_").concat("clientname.txt");
						Updater(filename,16,pg_row[i].hash_value,pg_row[i].offset,i);
						
						//Updates the Write position for the phone Attribute File
						filename=table.concat("_").concat("phone.txt");
						Updater(filename,12,pg_row[i].hash_value,pg_row[i].offset,i);
						
						//LOG SWAP OUT
						logger.info("SWAP OUT T-"+pg_row[i].table+" P-"+pg_row[i].offset/512+" B-"+pg_row[i].hash_value+"\n");
						
						pg_row[i]=new PageTable();
						
						empty=true;
						empty_positions[i]=1;
					}
				}
			}
			
			//Check For Empty Slots
			if(empty && !storeflag){
				for(int i=0;i<empty_positions.length;i++){
					if(empty_positions[i]==1){
						//Get The Page From Memory To Insert (Will be the last page of that Hash Value)
						ByteBuffer newPage=getLast15Records(hash_value,id,table,i);
						
						//Duplicate Found Check
						if(dup_flag==true){
							//LOG DUPLICATE FOUND
							logger.info("Insert Aborted, PK IC: Duplicate Record Found = "+id);
							dup_flag=false;
							return;
						}
						
						//LOG SWAP IN
						logger.info("SWAP IN T-"+pg_row[i].table+" P-"+pg_row[i].offset/512+" B-"+pg_row[i].hash_value+"\n");
						
						//Copy new page into memory buffer (Has to have space to store)
						for(int j=0;j<512;j++){
							row_buff.put((i*512)+j, newPage.get(j));						
						}
						
						//Get end value
						end=(i*512)+504;
						end_value=row_buff.getInt(end);
						
						//Insert into the new page
						//Store and modify the page table entry
						//ID
						row_buff=row_buff.putInt((i*512)+end_value, id);
						end_value=end_value+4;
						row_buff=row_buff.putInt(end, end_value);
						
							//NAME
						for(int loop=0;loop<16;loop++){
							row_buff=row_buff.put((i*512)+end_value+loop, name_bytes[loop]);
						}
						end_value=end_value+16;
						row_buff=row_buff.putInt(end, end_value);
						
							//PHONE
						row_buff=row_buff.putInt((i*512)+end_value, phone1);
						row_buff=row_buff.putInt((i*512)+end_value+4, phone2);
						row_buff=row_buff.putInt((i*512)+end_value+8, phone3);
						end_value=end_value+12;
						row_buff=row_buff.putInt(end, end_value);
						
						//Set Dirty Bit
						pg_row[i].dirty_bit=1;
						pg_row[i].time=System.currentTimeMillis();
						
						//Insertion Completed
						storeflag=true;
						
						//LOG INSERTED
						logger.info("Inserted: "+id+", "+name+", "+phone+"\n");
						
						break;
					}
				}
			}
			
			//No Match Exists or Match Did not help and No Empty Slots are available
			if(!storeflag){
				
				//Now we swap
				int i=swap_row();
				
				//LOG SWAP OUT
				logger.info("SWAP OUT T-"+pg_row[i].table+" P-"+pg_row[i].offset/512+" B-"+pg_row[i].hash_value+"\n");
				
				//Replace and Insert
				//Get The Page From Memory To Insert (Will be the last page of that Hash Value)
				ByteBuffer newPage=getLast15Records(hash_value,id,table,i);
				
				//Duplicate Found Check
				if(dup_flag==true){
					//LOG DUPLICATE FOUND
					logger.info("Insert Aborted, PK IC: Duplicate Record Found = "+id);
					dup_flag=false;
					return;
				}
				

				//Copy new page into memory buffer (Has to have space to store)
				for(int j=0;j<512;j++){
					row_buff.put((i*512)+j, newPage.get(j));						
				}
				
				//LOG SWAP IN
				logger.info("SWAP IN T-"+pg_row[i].table+" P-"+pg_row[i].offset/512+" B-"+pg_row[i].hash_value+"\n");
				
				//Get end value
				end=(i*512)+504;
				end_value=row_buff.getInt(end);
				
				//Insert into the new page
				//Store and modify the page table entry
				//ID
				row_buff=row_buff.putInt((i*512)+end_value, id);
				end_value=end_value+4;
				row_buff=row_buff.putInt(end, end_value);
				
					//NAME
				for(int loop=0;loop<16;loop++){
					row_buff=row_buff.put((i*512)+end_value+loop, name_bytes[loop]);
				}
				end_value=end_value+16;
				row_buff=row_buff.putInt(end, end_value);
				
					//PHONE
				row_buff=row_buff.putInt((i*512)+end_value, phone1);
				row_buff=row_buff.putInt((i*512)+end_value+4, phone2);
				row_buff=row_buff.putInt((i*512)+end_value+8, phone3);
				end_value=end_value+12;
				row_buff=row_buff.putInt(end, end_value);
				
				//Set Dirty Bit
				pg_row[i].dirty_bit=1;
				pg_row[i].time=System.currentTimeMillis();
				
				//Insertion Completed
				storeflag=true;
				
				//LOG INSERTED
				logger.info("Inserted: "+id+", "+name+", "+phone+"\n");
			}
		
	}
			
		private static int swap_row() throws IOException{
			
			//Initialize Variable
			String table,filename;
			
			//Implement LRU
			//Find LRU page from the pageTable
			int lru_page=0;
			long min=pg_row[0].time;
			for(int i=0;i<pg_row.length-1;i++){
				if(pg_row[i].time<min){
					min=pg_row[i].time;
					lru_page=i;
				}
			}
			
			String[] info_parts=null;
			//Check if dirty bit is 1 for Updating Physical Storage
			if(pg_row[lru_page].dirty_bit==1){
				table=pg_row[lru_page].table;
				
				//Find the corresponding MetaData
				for(int i=0;i<metaData.length;i++){
					info_parts=metaData[i].info.split(";");
					if(info_parts[0].contentEquals(table)){
						break;
					}
				}
				
				String[] attributes=info_parts[1].split(" ");
				String[] attributes_size=info_parts[2].split(" ");
				
				//Write into the physical memory
				for(int i=0;i<attributes.length;i++){
					filename=table.concat("_").concat(attributes[i]).concat(".txt");
					
					//Updates the Write position for the File
					Updater(filename,Integer.parseInt(attributes_size[i]),pg_row[lru_page].hash_value,pg_row[lru_page].offset,lru_page);
					
				}
			
			}
			
			return lru_page;
		}
		
		private static void Updater(String filename, int attr_size,int hash_value,int offset,int lru_page) throws IOException {
		
			//OPEN THE FILE
			File file=new File(filename);
			RandomAccessFile raf=new RandomAccessFile(file, "rwd");
			
			int nor=0;
			//Manage Offset
			if(attr_size!=4){
				//Find the number of records corresponding to attr_size = 4
				nor=(offset/512)*(504/4);
				nor=nor+((offset%512)/4);
			}
			
			//FOR NAME
			int buckets,rem;
			if(attr_size==16){
				buckets=nor/31;
				rem=nor%31;
				offset=(buckets*512)+(rem*16);
			}
			
			//FOR PHONE
			if(attr_size==12){
				buckets=nor/42;
				rem=nor%42;
				offset=(buckets*512)+(rem*12);
			}
			
			//Get Write Location
			int jumps=offset/512;
			
			int initial_pos;
			int pointer,pointer_value;
			
			initial_pos=64+(hash_value*512);
			
			while(jumps!=0){
				
				pointer=initial_pos+508;
				raf.seek(pointer);
				pointer_value=raf.readInt();
				initial_pos=64+(pointer_value*512);
				jumps=jumps-1;
			}
			
			//Write into file
			int wr_location=initial_pos+offset%512;
			raf.seek(wr_location);
			
			/***FOR ID*/
			if(attr_size==4){
				//get end pointer to know the size to write back
				int end_value=row_buff.getInt((lru_page*512)+504);
				int records=end_value/32;
				
				//To find out if it skips to next bucket
				int space_rem=504-(offset%512);
				int space_needed=records*4;
				
				byte[] write_from=new byte[512];
				for(int k=0;k<write_from.length;k++){
					write_from[k]=row_buff.get((lru_page*512)+k);
				}
				
				//Change buckets
				if(space_needed>space_rem){
					
					int records_in_next_bucket=(space_needed-space_rem)/4;
							
					//Write Space remaining records
					for(int i=0;i<records-records_in_next_bucket;i++){
						raf.write(write_from, i*32, 4);
					}
					
					int overflow=row_buff.getInt((lru_page*512)+508);
					if(overflow==-1){
						int new_bucket=((int)raf.length()-64)/512;
						raf.seek(initial_pos+508);
						raf.writeInt(new_bucket);
						
						//Initialize the new bucket
						raf.seek(64+(new_bucket*512)+504);
						raf.writeInt(0);
						raf.seek(64+(new_bucket*512)+508);
						raf.writeInt(-1);
						
						raf.seek(64+(new_bucket*512));
					}else{
						//Get the next bucket position
						raf.seek(64+(overflow*512));
					}
					
					//Write remaining records
					for(int i=records-records_in_next_bucket;i<records;i++){
						raf.write(write_from, i*32, 4);
					}
					
				}else{//Dont Change Buckets
				
					for(int i=0;i<records;i++){
						raf.write(write_from, i*32, 4);
					}
				}
				//Rewrite End Pointer
				int bucket=row_buff.getInt((lru_page*512)+508);
				raf.seek(64+(bucket*512)+504);
				int end_pointer=raf.readInt();
				raf.seek(64+(bucket*512)+504);
				raf.writeInt(end_pointer+4);
			}
			
			
			/***FOR NAME*/
			if(attr_size==16){
				//get end pointer to know the size to write back
				int end_value=row_buff.getInt((lru_page*512)+504);
				int records=end_value/32;
				
				//To find out if it skips to next bucket
				int space_rem=496-(offset%512);
				int space_needed=records*16;
				
				byte[] write_from=new byte[512];
				for(int k=0;k<write_from.length;k++){
					write_from[k]=row_buff.get((lru_page*512)+k);
				}
				
				//Change buckets
				if(space_needed>space_rem){
					
					int records_in_next_bucket=(space_needed-space_rem)/16;
							
					//Write remaining records in available space
					for(int i=0;i<records-records_in_next_bucket;i++){
						raf.write(write_from, (i*32)+4, 16);
					}
					
					int overflow=row_buff.getInt((lru_page*512)+508);
					if(overflow==-1){
						int new_bucket=((int)raf.length()-64)/512;
						raf.seek(initial_pos+508);
						raf.writeInt(new_bucket);
						
						//Initialize the new bucket
						raf.seek(64+(new_bucket*512)+504);
						raf.writeInt(0);
						raf.seek(64+(new_bucket*512)+508);
						raf.writeInt(-1);
						
						raf.seek(64+(new_bucket*512));
					}else{
						//Get the next bucket position
						raf.seek(64+(overflow*512));
					}
					
					//Write remaining records
					for(int i=records-records_in_next_bucket;i<records;i++){
						raf.write(write_from, (i*32)+4, 16);
					}
					
				}else{//Dont Change Buckets
				
					for(int i=0;i<records;i++){
						raf.write(write_from, (i*32)+4, 16);
					}
				}
				
				//Rewrite End Pointer
				int bucket=(initial_pos-64)/512;
				raf.seek(64+(bucket*512)+504);
				int end_pointer=raf.readInt();
				raf.seek(64+(bucket*512)+504);
				raf.writeInt(end_pointer+16);
			}
			
			
			/***FOR PHONE*/
			if(attr_size==12){
				//get end pointer to know the size to write back
				int end_value=row_buff.getInt((lru_page*512)+504);
				int records=end_value/32;
				
				//To find out if it skips to next bucket
				int space_rem=504-(offset%512);
				int space_needed=records*12;
				
				byte[] write_from=new byte[512];
				for(int k=0;k<write_from.length;k++){
					write_from[k]=row_buff.get((lru_page*512)+k);
				}
				
				//Change buckets
				if(space_needed>space_rem){
					
					int records_in_next_bucket=(space_needed-space_rem)/12;
							
					//Write Space remaining records
					for(int i=0;i<records-records_in_next_bucket;i++){
						raf.write(write_from, (i*32)+20, 12);
					}
					
					int overflow=row_buff.getInt((lru_page*512)+508);
					if(overflow==-1){
						int new_bucket=((int)raf.length()-64)/512;
						raf.seek(initial_pos+508);
						raf.writeInt(new_bucket);
						
						//Initialize the new bucket
						raf.seek(64+(new_bucket*512)+504);
						raf.writeInt(0);
						raf.seek(64+(new_bucket*512)+508);
						raf.writeInt(-1);
						
						raf.seek(64+(new_bucket*512));
					}else{
						//Get the next bucket position
						raf.seek(64+(overflow*512));
					}
					
					//Write remaining records
					for(int i=records-records_in_next_bucket;i<records;i++){
						raf.write(write_from, (i*32)+20, 12);
					}
					
				}else{//Dont Change Buckets
				
					for(int i=0;i<records;i++){
						raf.write(write_from, (i*32)+20, 12);
					}
				}
				
				//Rewrite End Pointer
				int bucket=(initial_pos-64)/512;
				raf.seek(64+(bucket*512)+504);
				int end_pointer=raf.readInt();
				raf.seek(64+(bucket*512)+504);
				raf.writeInt(end_pointer+12);
			}
			
			raf.close();
			
		}
		
		private static ByteBuffer getNext15Records(int hash_value, String table) throws IOException{
			
			//Start Reading the pages from the initial Page
			//Depending on read_counter you know the number of records already read
			
			ByteBuffer read=ByteBuffer.allocateDirect(512);
			boolean last_15=false;
			
			/***Get ID*/
			//Convert table to filename
			String filename=table.concat("_id").concat(".txt");
			
			//OPEN THE ID FILE
			File file=new File(filename);
			RandomAccessFile raf=new RandomAccessFile(file, "rwd");
			
			//Initialize Variables
			int pos=0;
			int start_location=64+(hash_value*512);
			int nor=read_counter*15;
			int overflow=hash_value;
			
			//Set starting position
			for(int i=0;i<nor;i++){
				pos=pos+4;
				if(pos==504){
					//get the jump location
					raf.seek(start_location+508);
					overflow=raf.readInt();
					start_location=64+(overflow*512);
					
					//reset pos
					pos=0;
				}
			}
			
			//Set OverFlow Value
			read.putInt(508, overflow);
			
			//Get End Location
			raf.seek(start_location+504);
			int end=raf.readInt();
			
			if(end==504){
				//Find if jump needed
				int jump_needed=end-pos;
				
				if(jump_needed<(15*4)){
					//You will Jump while Reading if overflow present
					raf.seek(start_location+508);
					overflow=raf.readInt();
					if(overflow==-1){
						//No overflow present, read what is there
						
						int to_read=(end-pos)/4;
						//Start Reading from here
						raf.seek(start_location+pos);
						
						for(int i=0;i<to_read;i++){
							read.putInt(i*32,raf.readInt());
						}
						
						//Set End and Overflow
						read.putInt(504, (to_read*32));
						//Overflow set above
						
						last_15=true;
						
					}else{
						//Jump while reading to
						int new_position=64+(overflow*512);
						
						//Write what is there
						int to_read=(end-pos)/4;
						//Start Reading from here
						raf.seek(start_location+pos);
						
						for(int i=0;i<to_read;i++){
							read.putInt(i*32,raf.readInt());
						}
						
						//Jump and write remaining
						raf.seek(new_position);
						int rem=15-to_read;
						
						//Check How many records are present again
						raf.seek(new_position+504);
						end=raf.readInt();
						
						int present=end/4;
						
						if(present<rem){
							//read how many records are present
							raf.seek(new_position);
							
							for(int i=to_read;i<to_read+present;i++){
								read.putInt(i*32,raf.readInt());
							}
							
							//Set End and Overflow
							read.putInt(504, (to_read+present)*32);
							read.putInt(508, overflow);
							
							last_15=true;
						}else{
							//read 15-rem records
							raf.seek(new_position);
							
							for(int i=to_read;i<to_read+rem;i++){
								read.putInt(i*32,raf.readInt());
							}
							
							//Set End and Overflow
							read.putInt(504, 504);
							read.putInt(508, overflow);
							
						}
						
					}
				}else{
					//Read The next 15 Records to Buffer
					
					//The last 15 if overflow is -1
					if(jump_needed==(15*4)){
						raf.seek(start_location+508);
						int overflow_check=raf.readInt();
						if(overflow_check==-1){
							last_15=true;
						}
					}
					
					//Start Reading from here
					raf.seek(start_location+pos);
					
					for(int i=0;i<15;i++){
						read.putInt(i*32,raf.readInt());
					}
					
					//Set End and Overflow (If all 15 written then set to 504)
					read.putInt(504, 504);
					read.putInt(508, overflow);
				}
				
			}else{
				//Read The next 15 Records to Buffer if there are 15
				int to_read=(end-pos)/4;
				
				if(to_read>=15){
					
					if(to_read==15){
						last_15=true;
					}
					
					//Start Reading from here
					raf.seek(start_location+pos);
				
					for(int i=0;i<15;i++){
						read.putInt(i*32,raf.readInt());
					}
				
					//Set End and Overflow (Overflow Written Above) (If all 15 written then set to 504)
					read.putInt(504, 504);
					//raf.seek(start_location+508);
					//read.putInt(508, raf.readInt());
				}else{
					//Write how many ever present
					raf.seek(start_location+pos);
					for(int i=0;i<to_read;i++){
						read.putInt(i*32,raf.readInt());
					}
					
					//Set End and Overflow (Overflow Written Above) (If all 15 written then set to 504)
					read.putInt(504, to_read*32);
					
					last_15=true;
					//raf.seek(start_location+508);
					//read.putInt(508, raf.readInt());
				}
			}
			
			raf.close();
			
			/***Get NAME*/
			//Convert table to filename
			filename=table.concat("_clientname").concat(".txt");
			
			//OPEN THE ID FILE
			file=new File(filename);
			raf=new RandomAccessFile(file, "rwd");
			
			//Set starting position
			pos=0;
			start_location=64+(hash_value*512);
			nor=read_counter*15;
			for(int i=0;i<nor;i++){
				pos=pos+16;
				if(pos==496){
					//get the jump location
					raf.seek(start_location+508);
					start_location=64+(raf.readInt()*512);
					
					//reset pos
					pos=0;
				}
			}
			
			//Get End Location
			raf.seek(start_location+504);
			end=raf.readInt();
				
			if(end==504){
				//Find if jump needed
				int jump_needed=end-pos;
				
				if(jump_needed<(15*16)){
					//You will Jump while Reading if overflow present
					raf.seek(start_location+508);
					overflow=raf.readInt();
					if(overflow==-1){
						//No overflow present, read what is there
						
						int to_read=(end-pos)/16;
						//Start Reading from here
						raf.seek(start_location+pos);
						
						for(int i=0;i<to_read;i++){
							for(int j=0;j<16;j++){
								read.put((i*32)+4+j, raf.readByte());
							}
						}
						
					}else{
						//Jump while reading to
						int new_position=64+(overflow*512);
						
						//Write what is there
						int to_read=(end-pos)/16;
						//Start Reading from here
						raf.seek(start_location+pos);
						
						for(int i=0;i<to_read;i++){
							for(int j=0;j<16;j++){
								read.put((i*32)+4+j, raf.readByte());
							}
						}
						
						//Jump and write remaining
						raf.seek(new_position);
						int rem=15-to_read;
						
						//Check How many records are present again
						raf.seek(new_position+504);
						end=raf.readInt();
						
						int present=end/16;
						
						if(present<rem){
							//read how many records are present
							raf.seek(new_position);
							
							for(int i=to_read;i<to_read+present;i++){
								for(int j=0;j<16;j++){
									read.put((i*32)+4+j, raf.readByte());
								}
							}
							
						}else{
							//read 15-rem records
							raf.seek(new_position);
							
							for(int i=to_read;i<to_read+rem;i++){
								for(int j=0;j<16;j++){
									read.put((i*32)+4+j, raf.readByte());
								}
							}
							
						}
						
					}
				}else{
					//IF end NOT EQUAL TO 504
					//Read The next 15 Records to Buffer
					
					//Start Reading from here
					raf.seek(start_location+pos);
					
					for(int i=0;i<15;i++){
						for(int j=0;j<16;j++){
							read.put((i*32)+4+j, raf.readByte());
						}
					}
				}
				
			}else{
				//Read The next 15 Records to Buffer if there are 15
				int to_read=(end-pos)/16;
				
				if(to_read>=15){
					//Start Reading from here
					raf.seek(start_location+pos);
				
					for(int i=0;i<15;i++){
						for(int j=0;j<16;j++){
							read.put((i*32)+4+j, raf.readByte());
						}
					}
				
				}else{
					//Write how many ever present
					raf.seek(start_location+pos);

					for(int i=0;i<to_read;i++){
						for(int j=0;j<16;j++){
							read.put((i*32)+4+j, raf.readByte());
						}
					}
					
				}
			}
			
			raf.close();
			
			/***Get PHONE*/
			//Convert table to filename
			filename=table.concat("_phone").concat(".txt");
			
			//OPEN THE PHONE FILE
			file=new File(filename);
			raf=new RandomAccessFile(file, "rwd");
			
			//Set starting position
			pos=0;
			start_location=64+(hash_value*512);
			nor=read_counter*15;
			for(int i=0;i<nor;i++){
				pos=pos+12;
				if(pos==504){
					//get the jump location
					raf.seek(start_location+508);
					start_location=64+raf.readInt()*512;
					
					//reset pos
					pos=0;
				}
			}
			
			//Get End Location
			raf.seek(start_location+504);
			end=raf.readInt();
				
			if(end==504){
				//Find if jump needed
				int jump_needed=end-pos;
				
				if(jump_needed<(15*12)){
					//You will Jump while Reading if overflow present
					raf.seek(start_location+508);
					overflow=raf.readInt();
					if(overflow==-1){
						//No overflow present, read what is there
						
						int to_read=(end-pos)/12;
						//Start Reading from here
						raf.seek(start_location+pos);
						
						for(int i=0;i<to_read;i++){
							read.putInt((i*32)+20,raf.readInt());
							read.putInt((i*32)+24,raf.readInt());
							read.putInt((i*32)+28,raf.readInt());
						}
						
					}else{
						//Jump while reading to
						int new_position=64+(overflow*512);
						
						//Write what is there
						int to_read=(end-pos)/12;
						//Start Reading from here
						raf.seek(start_location+pos);
						
						for(int i=0;i<to_read;i++){
							read.putInt((i*32)+20,raf.readInt());
							read.putInt((i*32)+24,raf.readInt());
							read.putInt((i*32)+28,raf.readInt());
						}
						
						//Jump and write remaining
						raf.seek(new_position);
						int rem=15-to_read;
						
						//Check How many records are present again
						raf.seek(new_position+504);
						end=raf.readInt();
						
						int present=end/12;
						
						if(present<rem){
							//read how many records are present
							raf.seek(new_position);
							
							for(int i=to_read;i<to_read+present;i++){
								read.putInt((i*32)+20,raf.readInt());
								read.putInt((i*32)+24,raf.readInt());
								read.putInt((i*32)+28,raf.readInt());
							}
							
						}else{
							//read 15-rem records
							raf.seek(new_position);
							
							for(int i=to_read;i<to_read+rem;i++){
								read.putInt((i*32)+20,raf.readInt());
								read.putInt((i*32)+24,raf.readInt());
								read.putInt((i*32)+28,raf.readInt());
							}
						}
						
					}
				}else{
					//IF end NOT EQUAL TO 504
					//Read The next 15 Records to Buffer
					
					//Start Reading from here
					raf.seek(start_location+pos);
					
					for(int i=0;i<15;i++){
						read.putInt((i*32)+20,raf.readInt());
						read.putInt((i*32)+24,raf.readInt());
						read.putInt((i*32)+28,raf.readInt());
					}
				}
				
			}else{
				//Read The next 15 Records to Buffer if there are 15
				int to_read=(end-pos)/12;
				
				if(to_read>=15){
					//Start Reading from here
					raf.seek(start_location+pos);
				
					for(int i=0;i<15;i++){
						read.putInt((i*32)+20,raf.readInt());
						read.putInt((i*32)+24,raf.readInt());
						read.putInt((i*32)+28,raf.readInt());
					}
				
				}else{
					//Write how many ever present
					raf.seek(start_location+pos);

					for(int i=0;i<to_read;i++){
						read.putInt((i*32)+20,raf.readInt());
						read.putInt((i*32)+24,raf.readInt());
						read.putInt((i*32)+28,raf.readInt());
					}
					
				}
			}
			
		
			//Increment Read Counter (Maintains How many Blocks of Records You have read ~ offset)
			read_counter=read_counter+1;
			
			if(last_15){
				read_counter=-1;
			}
			
			raf.close();
			
			return read;
		}
	
		private static ByteBuffer getLast15Records(int hash_value, int d_id, String table, int lru) throws IOException{
			
			//Get the last Page
			//Eventually you will return a page that has some empty slots to write in
			//If the last page is completely full, then create a new bucket for that attribute and
			//return an empty page 
			
			//Initialize buffer that will contain the page you return
			ByteBuffer read=ByteBuffer.allocateDirect(512);
			
			//Convert table to filename
			String filename=table.concat("_id").concat(".txt");
			
			//OPEN THE ID FILE
			File file=new File(filename);
			RandomAccessFile raf=new RandomAccessFile(file, "rwd");
			
			//Initialize variables
			int jump=0;
			int end_value,total_bytes_moved,num_records,records_onLastPage;
			
			//Find start position to start finding the last page
			int start_position=64+(hash_value*512);
			
						//Running Duplicate Check in the first Bucket (Isolated Code, no existing variables touched)
						raf.seek(start_position+504);
						int d_temp=0;
						int d_fblock_end=raf.readInt();
						int d_num_records=d_fblock_end/4;
						for(int di=0;di<d_num_records;di++){
							raf.seek(start_position+(di*4));
							d_temp=raf.readInt();
							if(d_id==d_temp){
								dup_flag=true;
								raf.close();
								return read;
							}
						}
			
			//check if this is the last bucket
			raf.seek(start_position+508);
			int overflow=raf.readInt();
			
			//Keep Jumping till you find the last page
			while(overflow!=-1){
				//Jumped
				jump=jump+1;
				
				//Reset start position
				start_position=64+(overflow*512);
				
								//Running Duplicate Check in the all chained Buckets (Isolated Code, no existing variables touched)
								raf.seek(start_position+504);
								d_temp=0;
								d_fblock_end=raf.readInt();
								d_num_records=d_fblock_end/4;
								for(int di=0;di<d_num_records;di++){
									raf.seek(start_position+(di*4));
									d_temp=raf.readInt();
									if(d_id==d_temp){
										dup_flag=true;
										raf.close();
										return read;
									}
								}
				
				//check if this is the last bucket
				raf.seek(start_position+508);
				overflow=raf.readInt();
			}
			
			//Reached the last bucket page
			//Now find the total number of records
			raf.seek(start_position+504);
			end_value=raf.readInt();
			int offset= (jump*512)+end_value;
			total_bytes_moved=(jump*504)+end_value;
			num_records=total_bytes_moved/4;
			
			//After you keep on transferring 15 for each page, last_page_value will give you how many remain
			records_onLastPage=num_records%15;
			
			//IF 0, check end_value, if that is 504, then create a new page for the bucket and return that
			if(records_onLastPage==0){
				if(end_value==504){
					int new_bucket=((int)raf.length()-64)/512;
					raf.seek(start_position+508);
					raf.writeInt(new_bucket);
					
					//Initialize the new bucket
					raf.seek(64+(new_bucket*512)+504);
					raf.writeInt(0);
					raf.seek(64+(new_bucket*512)+508);
					raf.writeInt(-1);
					
					//Log creation of a new page
					logger.info("Created New Page: P-"+(jump+1)+" B-"+hash_value);
					
					//Set end and overflow pointers for the page to return
					read.putInt(504, 0);
					read.putInt(508,new_bucket);
					
				}else{
					//Return an empty buffer but set offset in Page Table Correctly
					//Set end and overflow pointers for the page to return
					read.putInt(504, 0);
					read.putInt(508,((start_position-64)/512));
					
				}
			}else{
				//Else Now for the remaining, you need to figure out if they lie in the same page or between pages
				if((records_onLastPage*4)<=end_value){
					//same page
					if(end_value==504){
						int new_bucket=(int)(raf.length()-64)/512;
						raf.seek(start_position+508);
						raf.writeInt(new_bucket);
						
						//Initialize the new bucket
						raf.seek(64+(new_bucket*512)+504);
						raf.writeInt(0);
						raf.seek(64+(new_bucket*512)+508);
						raf.writeInt(-1);
						
						//Log creation of a new page
						logger.info("Created New Page: P-"+(jump+1)+" B-"+hash_value);
						
						//Put existing values into our return buffer
						raf.seek(start_position+end_value-(records_onLastPage*4));
						for(int i=0;i<records_onLastPage;i++){
							read.putInt((i*32),raf.readInt());
						}
						
						//Set end and overflow pointers for the page to return
						read.putInt(504, records_onLastPage*32);
						read.putInt(508,new_bucket); 

					}else{
						//Put existing values into our return buffer
						raf.seek(start_position+end_value-(records_onLastPage*4));
						for(int i=0;i<records_onLastPage;i++){
							read.putInt((i*32),raf.readInt());
						}
						
						//Set end and overflow pointers for the page to return
						read.putInt(504, records_onLastPage*32);
						read.putInt(508,((start_position-64)/512));  
						
					}
				}else{
					//different pages
					
					//Accounting for 8 bytes of pointer storage
					//offset=offset-8;
					
					//check how many on the current page
					int on_current=end_value/4;
					int on_previous=records_onLastPage-on_current;
					
					//Write previous page records into the return buffer
					//Setting up location
					int previous_position=64+(hash_value*512);
					for(int i=0;i<jump-1;i++){
						raf.seek(previous_position+508);
						previous_position=64+(raf.readInt()*512);
					}
					
					//Write previous page values to return buffer
					raf.seek(previous_position+(504-(on_previous*4)));
					for(int i=0;i<on_previous;i++){
						read.putInt((i*32),raf.readInt());
					}
					
					//Write Current page values to return buffer
					raf.seek(start_position);
					for(int i=on_previous;i<on_current+on_previous;i++){
						read.putInt((i*32),raf.readInt());
					}
					
					//Set end and overflow pointers for the page to return
					read.putInt(504, records_onLastPage*32);
					read.putInt(508,((start_position-64)/512)); 
				}
			}
			
			raf.close();
			
			/***NAME*/
			//Convert table to filename
			filename=table.concat("_clientname").concat(".txt");
			
			//OPEN THE ID FILE
			file=new File(filename);
			raf=new RandomAccessFile(file, "rwd");
			
			//Initialize variables
			jump=0;
			end_value=0;total_bytes_moved=0;num_records=0;records_onLastPage=0;
			
			//Find start position to start finding the last page
			start_position=64+(hash_value*512);
			
			//check if this is the last bucket
			raf.seek(start_position+508);
			overflow=raf.readInt();
			
			//Keep Jumping till you find the last page
			while(overflow!=-1){
				//Jumped
				jump=jump+1;
				
				//Reset start position
				start_position=64+(overflow*512);
				
				//check if this is the last bucket
				raf.seek(start_position+508);
				overflow=raf.readInt();
			}
			
			//Reached the last bucket page
			//Now find the total number of records
			raf.seek(start_position+504);
			end_value=raf.readInt();
			
			if(end_value==504){
				total_bytes_moved=(jump*496)+496;
			}else{
				total_bytes_moved=(jump*496)+end_value;
			}
			
			num_records=total_bytes_moved/16;
			
			//After you keep on transferring 15 for each page, last_page_value will give you how many remain
			records_onLastPage=num_records%15;
			
			//IF 0 check end_value, if that is 504, then create a new bucket and return that
			if(records_onLastPage==0){
				if(end_value==504){
					int new_bucket=((int)raf.length()-64)/512;
					raf.seek(start_position+508);
					raf.writeInt(new_bucket);
					
					//Initialize the new bucket
					raf.seek(64+(new_bucket*512)+504);
					raf.writeInt(0);
					raf.seek(64+(new_bucket*512)+508);
					raf.writeInt(-1);
					
				}
			}else{
				//Else Now for the remaining, you need to figure out if they lie in the same page or between pages
				if((records_onLastPage*16)<=end_value){
					//same page
					if(end_value==504){
						int new_bucket=((int)raf.length()-64)/512;
						raf.seek(start_position+508);
						raf.writeInt(new_bucket);
						
						//Initialize the new bucket
						raf.seek(64+(new_bucket*512)+504);
						raf.writeInt(0);
						raf.seek(64+(new_bucket*512)+508);
						raf.writeInt(-1);
						
						//Put existing values into our return buffer
						raf.seek(start_position+end_value-(records_onLastPage*16));
						for(int i=0;i<records_onLastPage;i++){
							for(int j=0;j<16;j++){
								read.put((i*32)+4+j, raf.readByte());
							}
						}

					}else{
						//Put existing values into our return buffer
						raf.seek(start_position+end_value-(records_onLastPage*16));
						for(int i=0;i<records_onLastPage;i++){
							for(int j=0;j<16;j++){
								read.put((i*32)+4+j, raf.readByte());
							}
						}
						
					}
				}else{
					//different pages
					//check how many on the current page
					int on_current=end_value/16;
					int on_previous=records_onLastPage-on_current;
					
					//Write previous page records into the return buffer
					//Setting up location
					int previous_position=64+(hash_value*512);
					for(int i=0;i<jump-1;i++){
						raf.seek(previous_position+508);
						previous_position=64+(raf.readInt()*512);
					}
					
					//Write previous page values to return buffer
					raf.seek(previous_position+(504-(on_previous*16)));
					for(int i=0;i<on_previous;i++){
						for(int j=0;j<16;j++){
							read.put((i*32)+4+j, raf.readByte());
						}
					}
					
					//Write Current page values to return buffer
					raf.seek(start_position);
					for(int i=on_previous;i<on_current+on_previous;i++){
						for(int j=0;j<16;j++){
							read.put((i*32)+4+j, raf.readByte());
						}
					}
					
				}
			}
			
			raf.close();
			
			/***PHONE*/
			//Convert table to filename
			filename=table.concat("_phone").concat(".txt");
			
			//OPEN THE ID FILE
			file=new File(filename);
			raf=new RandomAccessFile(file, "rwd");
			
			//Initialize variables
			jump=0;
			end_value=0;total_bytes_moved=0;num_records=0;records_onLastPage=0;
			
			//Find start position to start finding the last page
			start_position=64+(hash_value*512);
			
			//check if this is the last bucket
			raf.seek(start_position+508);
			overflow=raf.readInt();
			
			//Keep Jumping till you find the last page
			while(overflow!=-1){
				//Jumped
				jump=jump+1;
				
				//Reset start position
				start_position=64+(overflow*512);
				
				//check if this is the last bucket
				raf.seek(start_position+508);
				overflow=raf.readInt();
			}
			
			//Reached the last bucket page
			//Now find the total number of records
			raf.seek(start_position+504);
			end_value=raf.readInt();
			total_bytes_moved=(jump*504)+end_value;
			num_records=total_bytes_moved/12;
			
			//After you keep on transferring 15 for each page, last_page_value will give you how many remain
			records_onLastPage=num_records%15;
			
			//IF 0 check end_value, if that is 504, then create a new bucket and return that
			if(records_onLastPage==0){
				if(end_value==504){
					int new_bucket=((int)raf.length()-64)/512;
					raf.seek(start_position+508);
					raf.writeInt(new_bucket);
					
					//Initialize the new bucket
					raf.seek(64+(new_bucket*512)+504);
					raf.writeInt(0);
					raf.seek(64+(new_bucket*512)+508);
					raf.writeInt(-1);
					
				}
			}else{
				//Else Now for the remaining, you need to figure out if they lie in the same page or between pages
				if((records_onLastPage*12)<=end_value){
					//same page
					if(end_value==504){
						int new_bucket=((int)raf.length()-64)/512;
						raf.seek(start_position+508);
						raf.writeInt(new_bucket);
						
						//Initialize the new bucket
						raf.seek(64+(new_bucket*512)+504);
						raf.writeInt(0);
						raf.seek(64+(new_bucket*512)+508);
						raf.writeInt(-1);
						
						//Put existing values into our return buffer
						raf.seek(start_position+end_value-(records_onLastPage*12));
						for(int i=0;i<records_onLastPage;i++){
							read.putInt((i*32)+20,raf.readInt());
							read.putInt((i*32)+24,raf.readInt());
							read.putInt((i*32)+28,raf.readInt());
						}

					}else{
						//Put existing values into our return buffer
						raf.seek(start_position+end_value-(records_onLastPage*12));
						for(int i=0;i<records_onLastPage;i++){
							read.putInt((i*32)+20,raf.readInt());
							read.putInt((i*32)+24,raf.readInt());
							read.putInt((i*32)+28,raf.readInt());
						}
						
					}
				}else{
					//different pages
					//check how many on the current page
					int on_current=end_value/12;
					int on_previous=records_onLastPage-on_current;
					
					//Write previous page records into the return buffer
					//Setting up location
					int previous_position=64+(hash_value*512);
					for(int i=0;i<jump-1;i++){
						raf.seek(previous_position+508);
						previous_position=64+(raf.readInt()*512);
					}
					
					//Write previous page values to return buffer
					raf.seek(previous_position+(504-(on_previous*12)));
					for(int i=0;i<on_previous;i++){
						read.putInt((i*32)+20,raf.readInt());
						read.putInt((i*32)+24,raf.readInt());
						read.putInt((i*32)+28,raf.readInt());
					}
					
					//Write Current page values to return buffer
					raf.seek(start_position);
					for(int i=on_previous;i<on_current+on_previous;i++){
						read.putInt((i*32)+20,raf.readInt());
						read.putInt((i*32)+24,raf.readInt());
						read.putInt((i*32)+28,raf.readInt());
					}
					
				}
			}
			
			raf.close();
			
			//SET PAGE TABLE
			pg_row[lru].table=table;
			pg_row[lru].dirty_bit=0;
			pg_row[lru].hash_value=hash_value;
			pg_row[lru].offset=offset-(records_onLastPage*4); //Position of first record written in the return buffer relative to the initial hash bucket value
			pg_row[lru].time=System.currentTimeMillis();
			
			return read;
		}
				
		protected static void convert_to_columnstore(String fileName,BufferedReader reader) throws IOException{
			
			//Get the Filename without extension
			String[] split_name=fileName.split("\\.");
			String name=split_name[0];
			
			//Create the directed files to store in
			String attrib1_file=name.concat("_").concat("id").concat(".txt");
			String attrib2_file=name.concat("_").concat("clientname").concat(".txt");
			String attrib3_file=name.concat("_").concat("phone").concat(".txt");
			
			//Create Files
			File file_id=new File(attrib1_file);
			File file_name=new File(attrib2_file);
			File file_phone=new File(attrib3_file);
			
			//Create the BufferedWriter instances for each file
			RandomAccessFile raf_id=new RandomAccessFile(file_id, "rw");
			RandomAccessFile raf_name=new RandomAccessFile(file_name, "rw");
			RandomAccessFile raf_phone=new RandomAccessFile(file_phone, "rw");
			
			//ByteBuffer Stores all data, to be flushed to file
			int header_size=64;
			int buffer_size_id=8258; //16 x 512 = 8192 + 64 for bucket header
			int buffer_size_name=8258; //16 x 512 = 8192 + 64 for bucket header
			int buffer_size_phone=8258; //16 x 512 = 8192 + 64 for bucket header
			ByteBuffer id_buffer = ByteBuffer.allocateDirect(buffer_size_id);
			ByteBuffer name_buffer = ByteBuffer.allocateDirect(buffer_size_name);
			ByteBuffer phone_buffer = ByteBuffer.allocateDirect(buffer_size_phone);
			
			//Assuming each bucket to be of size 1 page = 512 bytes
			//Initialize bucket pointers and Set end and overflow pointers
			for(int i=0;i<16;i++){
				//Bucket Pointers
				id_buffer=id_buffer.putInt(i*4, header_size+(i*512));
				name_buffer=name_buffer.putInt(i*4, header_size+(i*512));
				phone_buffer=phone_buffer.putInt(i*4, header_size+(i*512));
				
				//End Pointers
				id_buffer=id_buffer.putInt(header_size+(i*512)+504, 0);
				name_buffer=name_buffer.putInt(header_size+(i*512)+504, 0);
				phone_buffer=phone_buffer.putInt(header_size+(i*512)+504, 0);
				
				//Overflow pointers
				id_buffer=id_buffer.putInt(header_size+(i*512)+508, -1);
				name_buffer=name_buffer.putInt(header_size+(i*512)+508, -1);
				phone_buffer=phone_buffer.putInt(header_size+(i*512)+508, -1);
			}
			
			//Hash the values and put them into the resp. buckets
			int hash_value,location,end;
			int id,end_value_id;
			String client_name,phone_string;
			String line=reader.readLine();
			
			//Set No. of buckets initially, increases when overflow buckets are added
			int buckets_id=15,buckets_phone=15,buckets_name=15;
			int storeflag=1;
			
			//For ID
			int new_chain_location;
			int chain,chain_value_id;
			
			//For Name
			byte[] name_bytes=new byte[16];
			int end_value_name,chain_value_name;
			
			//For Phone
			int end_value_phone,chain_value_phone;
			
			while(line!=null){
				//split the row into the attributes
				String[] split_row=line.split(",");
				id=Integer.parseInt(split_row[0]);
				client_name=split_row[1];
				phone_string=split_row[2];
				
				//Running Check on Name, if length > 16 then abort the insertion of the row and log Error
				byte[] name_check=client_name.getBytes();
				
				//Check for length constraints
				if(name_check.length>16){
					//Log Errors
					continue;
				}
				
				//Hash the id to get the resp. bucket
				hash_value=id%16;
				
				//Get bucket Location from bucket header, here each page is 512 bytes
				location=hash_value*4;
				location=id_buffer.getInt(location);
				
				/***
				 * FOR ID ATRRIBUTE
				 */
				//Check end value, to know where to store and handle chaining
				
				end=location+504;
				end_value_id=id_buffer.getInt(end);
				new_chain_location=location;
				
				if(end_value_id==504){
					while(end_value_id==504){
						
						//check for chain
						chain=new_chain_location+508;
						chain_value_id=id_buffer.getInt(chain);
						
						//Create a New Page and chain if full and no overflow bucket present
						if(chain_value_id==-1){
							
							//Setting the Overflow in the existing bucket
							buckets_id=buckets_id+1;
							id_buffer=id_buffer.putInt(new_chain_location+508, buckets_id);
							
							//Increase Buffer size
							buffer_size_id=buffer_size_id+512;
							ByteBuffer temp_id_buffer = ByteBuffer.allocateDirect(buffer_size_id);
							temp_id_buffer.put(id_buffer);
							id_buffer=temp_id_buffer;
							temp_id_buffer.clear();
							
							//Initialize new page
							//End Pointers
							id_buffer=id_buffer.putInt(header_size+(buckets_id*512)+504, 0);
						
							//Overflow pointers
							id_buffer=id_buffer.putInt(header_size+(buckets_id*512)+508, -1);
							
							//set end location
							end=(header_size)+(buckets_id*512)+504;
							
							//Store Value in the new overflow bucket and update end value
							id_buffer=id_buffer.putInt(header_size+(buckets_id*512), id);
							id_buffer=id_buffer.putInt(end, 4);
							
							//do not store again
							storeflag=0;
							
							//Reset end value
							end_value_id=id_buffer.getInt(end);
							
						}else{  //Jump to the overflow bucket
							new_chain_location=header_size+(chain_value_id*512);
							//Reset end value
							end=new_chain_location+504;
							end_value_id=id_buffer.getInt(end);
						}
					}
					
					if(storeflag==1){
						//Store Value at end Location and update end value
						id_buffer=id_buffer.putInt(new_chain_location+end_value_id, id);
						id_buffer=id_buffer.putInt(end, end_value_id+4);
					}
					storeflag=1;
					
				}else{
					//Store Value at end Location and update end value
					id_buffer=id_buffer.putInt(new_chain_location+end_value_id, id);
					id_buffer=id_buffer.putInt(end, end_value_id+4);
				}
				

				/***
				 * FOR NAME ATRRIBUTE
				 */
				//Name stored in name_check variable as a byte array of length 16 or less
				//Format it to make it 16 always
				
				for(int loop=0;loop<name_check.length;loop++){
					name_bytes[loop]=name_check[loop];
				}
				
				String blank=" ";
				for(int loop=name_check.length;loop<16;loop++){
					name_bytes[loop]=blank.toString().getBytes()[0];
				}
				
				//Check end value, to know where to store and handle chaining
				end=location+504;
				end_value_name=name_buffer.getInt(end);
				new_chain_location=location;
				
				if(end_value_name==504){
					while(end_value_name==504){
						
						//check for chain
						chain=new_chain_location+508;
						chain_value_name=name_buffer.getInt(chain);
						
						//Create a New Page and chain if full and no overflow bucket present
						if(chain_value_name==-1){
							buckets_name=buckets_name+1;
							name_buffer=name_buffer.putInt(new_chain_location+508, buckets_name);
							
							//Increase Buffer size
							buffer_size_name=buffer_size_name+512;
							ByteBuffer temp_name_buffer = ByteBuffer.allocateDirect(buffer_size_name);
							temp_name_buffer.put(name_buffer);
							name_buffer=temp_name_buffer;
							temp_name_buffer.clear();
							
							//Initialize new page
							//End Pointers
							name_buffer=name_buffer.putInt(header_size+(buckets_name*512)+504, 0);
						
							//Overflow pointers
							name_buffer=name_buffer.putInt(header_size+(buckets_name*512)+508, -1);
							
							//set end location
							end=(header_size)+(buckets_name*512)+504;
							
							//Store Value in the new overflow bucket and update end value
							for(int loop=0;loop<16;loop++){
								name_buffer=name_buffer.put(header_size+(buckets_name*512)+loop, name_bytes[loop]);
							}
							name_buffer=name_buffer.putInt(end, 16);
							
							//do not store again
							storeflag=0;
							
							//Reset end value
							end_value_name=name_buffer.getInt(end);
							
						}else{  //Jump to the overflow bucket
							new_chain_location=header_size+(chain_value_name*512);
							//Reset end value
							end=new_chain_location+504;
							end_value_name=name_buffer.getInt(end);
						}
					}
					
					if(storeflag==1){
						//Store Value at end Location and update end value
						for(int loop=0;loop<16;loop++){
							name_buffer=name_buffer.put(new_chain_location+end_value_name+loop, name_bytes[loop]);
						}
						
						if(end_value_name+16==496){
							name_buffer=name_buffer.putInt(end, 504);
						}else{
							name_buffer=name_buffer.putInt(end, end_value_name+16);
						}
					}
					storeflag=1;
					
				}else{
					//Store Value at end Location and update end value
					for(int loop=0;loop<16;loop++){
						name_buffer=name_buffer.put(new_chain_location+end_value_name+loop, name_bytes[loop]);
					}
					if(end_value_name+16==496){
						name_buffer=name_buffer.putInt(end, 504);
					}else{
						name_buffer=name_buffer.putInt(end, end_value_name+16);
					}
				}
				
				/***
				 * FOR PHONE ATRRIBUTE
				 */
				//Change Format of Phone Attribute
				String[] phone_parts=phone_string.split("-");
				int phone1=Integer.parseInt(phone_parts[0]);
				int phone2=Integer.parseInt(phone_parts[1]);
				int phone3=Integer.parseInt(phone_parts[2]);

				//Check end value, to know where to store and handle chaining
				
				end=location+504;
				end_value_phone=phone_buffer.getInt(end);
				new_chain_location=location;
				
				if(end_value_phone==504){
					while(end_value_phone==504){
						
						//check for chain
						chain=new_chain_location+508;
						chain_value_phone=phone_buffer.getInt(chain);
						
						//Create a New Page and chain if full and no overflow bucket present
						if(chain_value_phone==-1){
							buckets_phone=buckets_phone+1;
							phone_buffer=phone_buffer.putInt(new_chain_location+508, buckets_phone);
							
							//Increase Buffer size
							buffer_size_phone=buffer_size_phone+512;
							ByteBuffer temp_phone_buffer = ByteBuffer.allocateDirect(buffer_size_phone);
							temp_phone_buffer.put(phone_buffer);
							phone_buffer=temp_phone_buffer;
							temp_phone_buffer.clear();
							
							//Initialize new page
							//End Pointers
							phone_buffer=phone_buffer.putInt(header_size+(buckets_phone*512)+504, 0);
						
							//Overflow pointers
							phone_buffer=phone_buffer.putInt(header_size+(buckets_phone*512)+508, -1);
							
							//set end location
							end=(header_size)+(buckets_phone*512)+504;
							
							//Store Value in the new overflow bucket and update end value
							phone_buffer=phone_buffer.putInt(header_size+(buckets_phone*512), phone1);
							phone_buffer=phone_buffer.putInt(header_size+(buckets_phone*512)+4, phone2);
							phone_buffer=phone_buffer.putInt(header_size+(buckets_phone*512)+8, phone3);
							phone_buffer=phone_buffer.putInt(end, 12);
							
							//do not store again
							storeflag=0;
							
							//Reset end value
							end_value_phone=phone_buffer.getInt(end);
							
						}else{  //Jump to the overflow bucket
							new_chain_location=header_size+(chain_value_phone*512);
							//Reset end value
							end=new_chain_location+504;
							end_value_phone=phone_buffer.getInt(end);
						}
					}
					
					if(storeflag==1){
						//Store Value at end Location and update end value if not stored
						phone_buffer=phone_buffer.putInt(new_chain_location+end_value_phone, phone1);
						phone_buffer=phone_buffer.putInt(new_chain_location+end_value_phone+4, phone2);
						phone_buffer=phone_buffer.putInt(new_chain_location+end_value_phone+8, phone3);
						phone_buffer=phone_buffer.putInt(end, end_value_phone+12);
					}
					storeflag=1;
					
				}else{
					//Store Value at end Location and update end value
					phone_buffer=phone_buffer.putInt(new_chain_location+end_value_phone, phone1);
					phone_buffer=phone_buffer.putInt(new_chain_location+end_value_phone+4, phone2);
					phone_buffer=phone_buffer.putInt(new_chain_location+end_value_phone+8, phone3);
					phone_buffer=phone_buffer.putInt(end, end_value_phone+12);
				}
				
				line=reader.readLine();
			}
			
			//Store Id file
			byte[] wri_buff = new byte[id_buffer.capacity()];
			id_buffer.get(wri_buff);
			raf_id.write(wri_buff);

			//Store Name file
			byte[] wrn_buff = new byte[name_buffer.capacity()];
			name_buffer.get(wrn_buff);
			raf_name.write(wrn_buff);
			
			//Store Phone file
			byte[] wrp_buff = new byte[phone_buffer.capacity()];
			phone_buffer.get(wrp_buff);
			raf_phone.write(wrp_buff);
			
			raf_id.close();
			raf_name.close();
			raf_phone.close();
		}
			
		protected static class PageTable{
			String table;
			int hash_value;
			int offset;
			int dirty_bit;
			long time;
			
			PageTable(){
				//Initilaze values, helps to check if entries exists
				table="";
				hash_value=-1;
				offset=-1;
				dirty_bit=-1;
				time=-1;
			}
		}
	
		protected static class MetaData{
			String info;
			MetaData(){
				info="";
			}
		}

}
