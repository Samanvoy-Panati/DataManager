package com.db.dataManger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class Scheduler1 {
	
	//Create Logger
	private static Logger logger;
	
	//WaitFor Graph
	protected static ArrayList<Node> wfg=new ArrayList<Node>();
	
	//Deadlocked T/P List
	protected static ArrayList<Integer> dl_list=new ArrayList<Integer>();
	
	//Transaction/Process ID List
	protected static ArrayList<Begin> tp_list=new ArrayList<Begin>();
	
	//Global Transaction/Process ID Counter
	protected static int global_tp_id=0;

	//Transaction Operation List
	protected static ArrayList<OpNode> op_list=new ArrayList<OpNode>();
	
	//Implementation of lock tables starts here
		//Array list which contains the list of transactions the present transaction are conflict with
		public static ArrayList<Integer> conflict_Transactions = new ArrayList<Integer>();
		 
		//three hash maps which contains the locks on PK,AC and Table
		public static List<LockNode> PK_list = new ArrayList<LockNode>();
		public static List<LockNode> AC_list = new ArrayList<LockNode>(); 
		public static List<LockNode> Table_list = new ArrayList<LockNode>(); 
		//Hash maps for locks on three values Primary Key(PK)--I,R, Area Code(AC)--G,M and Table--D
		public static HashMap<String, List<LockNode>> PK_Locks = new HashMap<String, List<LockNode>>();
		public static HashMap<String, List<LockNode>> AC_Locks = new HashMap<String, List<LockNode>>();
		public static HashMap<String, List<LockNode>> Table_Locks = new HashMap<String, List<LockNode>>();
	
	protected static void schedule(String query, String script) throws IOException{
		
		//check if Log File Exists
		String logname="ScheduleLog.log";
		File f=new File(logname);
		if(!f.exists()){
			//Create Log File
			Logger logger = Logger.getLogger("Scheduler");
			FileHandler fh = new FileHandler(logname);  
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();  
			fh.setFormatter(formatter);  
			logger.setUseParentHandlers(false);
		}
		
		//If Begin or Abort or Commit, Do necessary operation and move on
		String[] parts=query.split(" ");
		String op=parts[0];
		
		if(op.contentEquals("B") || op.contentEquals("C") || op.contentEquals("A")){
			
			if(op.contentEquals("B")){
				//Store in Transaction/Process ID List and Whether it is a Process or Transaction along with Script Name
				global_tp_id++;
				Begin b=new Begin();

				//updating logger 
				logger.info("Transaction with id "+global_tp_id+" started running");
				
				//Add the fields to the new Begin instance
				b.id=global_tp_id;
				b.type=Integer.parseInt(parts[1]);
				b.script=script;
				
				//Add to the tp_list
				tp_list.add(b);
				}else{
					if(op.contentEquals("C")){
						//Do all the operations of that Transaction or if Process then just remove from tp_list
						
						//To find the entry in tp_list just check for script name, because one script can execute only 1 T/P at a time
						for(int iter=0;iter<tp_list.size();iter++){
							if(tp_list.get(iter).script.contentEquals(script)){
								int remove_id=tp_list.get(iter).id;
								if(tp_list.get(iter).type==1){
								
									ArrayList<String> ops=new ArrayList<String>();
									//Find id in Op_list and Push all corresponding Operations
									for(int x=0;x<op_list.size();x++){
										if(op_list.get(x).id==remove_id){
											ops=op_list.get(x).op;
											break;
										}
									}
									
									//Push Operations one by one
									for(int x=0;x<ops.size();x++){
										DataManager.runQuery(ops.get(x),script);
									}
									
									//Remove Entry from tp_list
									tp_list.remove(iter);
									//Remove operation list
									for(int iter3=0;iter3<op_list.size();iter3++){
										if(op_list.get(iter3).id==remove_id){
											op_list.remove(iter3);
											break;
										}
									}
									}else{
										//Remove Entry from tp_list
										tp_list.remove(iter);
									}
								
								//Remove all Locks
								remove_locks(remove_id);

								//updating logger 
								logger.info("Transaction with id "+remove_id+" committed and changes are made permanent");
							
								//Remove from wfg
								for(int iter2=0;iter2<wfg.size();iter2++){
									if(wfg.get(iter2).trans==remove_id){
										wfg.remove(iter2);
										break;
									}
								}
								break;
							}
						}
					}else{
						//If Abort just throw away the operation List for that Transaction, if Process just Abort
						 
						//To find the entry in tp_list just check for script name, because one script can execute only 1 T/P at a time
						for(int iter=0;iter<tp_list.size();iter++){
							if(tp_list.get(iter).script.contentEquals(script)){
								int remove_id=tp_list.get(iter).id;
								
								//updating logger 
								logger.info("");
								//Remove from T/P list
								tp_list.remove(iter);
								
								//Remove Op_list
								for(int x=0;x<op_list.size();x++){
									if(op_list.get(x).id==remove_id){
										op_list.remove(x);
										break;
									}
								}
								
								//Remove from wfg
								for(int x=0;x<wfg.size();x++){
									if(wfg.get(x).trans==remove_id){
										wfg.remove(x);
										break;
									}
								}
								
								//Remove all Locks
								remove_locks(remove_id);

								//updating logger 
								logger.info("Deadlock Detected");
								logger.info("Transaction with id "+remove_id+" aborted due to deadlock");
								
							}
						}
					}
				}
					
			}else{
				//For Operation R,I,G,M,D
				int type=-1;
				int id=-1;
				
				//Find T/P
				for(int iter=0;iter<tp_list.size();iter++){
					if(tp_list.get(iter).script.contentEquals(script)){
						type=tp_list.get(iter).type;
						id=tp_list.get(iter).id;
						break;
					}
				}
				
				//Check For Conflicts
				if(LockTable(type, id, query, script)){
					
					//Find Index of ArrayList By ScriptName to block
					for(int x=0;x<Initialize.scriptNames.size();x++){
						if(Initialize.scriptNames.get(x).contentEquals(script)){
							Initialize.blockList.set(x, 1);
						}
					}
					
					//Insert into WFG
					insertWFG(id, conflict_Transactions);
				}else{
					//Check whether Transaction or Process
					if(type==1){
						boolean exists=false;
						int x=0;
						//Check if Op_List for that Transaction id already Exists
						for(x=0;x<op_list.size();x++){
							if(op_list.get(x).id==id){
								exists=true;
								break;
							}
						}
						
						//if op_list exists add to it else create a new OpNode
						if(exists){
							op_list.get(x).op.add(query);
						}else{
							OpNode o=new OpNode();
							o.id=id;
							o.op.add(query);
							op_list.add(o);
						}
						
					}else{
						DataManager.runQuery(query,script);
					}
					
				}
				
			}
		
	}
	
	/**To Block a Transaction or Process the Corresponding Script File should Block
	 *
	 * To Block Call the Following Functions:
	 * 
	 * 			int blockIndex=Initialize.scriptNames.indexOf(script);
				Initialize.blockList.set(blockIndex, 1);
				Initialize.blockQueryList.set(blockIndex, query);
				
	  To Unblock Call:
	  			You Should Know Which Script is Unblocking and follow the same procedure as Above
	  			****Carefull When unblocking set, Initialize.blockList.set(blockIndex, -1); and not 0
	  			****and do not set Initialize.blockQueryList
	
	 */
	
	/***
	 * Wait for graph functions listed here: Insert, Remove, Check Cycles
	 * Just Call insertWFG if the query results on conflict when checking the Lock Table
	 * The input is:
	 * trans --> Every query starting with B will be assigned a new transaction or process id number,
	 * 			 please set trans to that id number. (Do not assign Transactions and Processes the same id).
	 * 
	 * ArrayList<Integer> waitsFor --> The Transaction/process because of which the conflict occurred.
	 * 									Add all the id's of the conflicting Transaction/process to a
	 * 									ArrayList and pass as the second argument.
	 */
	
	private static void insertWFG(int trans, ArrayList<Integer> waitsFor){
		
		//Check if Transaction/Process mentioned in trans already exists
		boolean exists=false;
		int matchIndex=0;
		for(int iter=0;iter<wfg.size();iter++){
			if(wfg.get(iter).trans==trans){
				matchIndex=iter;
				exists=true;
				break;
			}
		}
		
		//If it exists
		if(exists==true){
			//add the waitFor elements to the existing nodes ArrayList
			//Because the same T/P will not wait for the same T/P we can
			//add just append it to the existing waitFor.
			ArrayList<Integer> waitList=wfg.get(matchIndex).waitsFor;
			
			for(int iter=0;iter<waitsFor.size();iter++){
				waitList.add(waitsFor.get(iter));
			}
			
			Node n=new Node(trans, waitList);
			wfg.set(matchIndex, n);
		}else{
			Node n=new Node(trans, waitsFor);
			wfg.add(n);
		}
		
		//Check for Deadlocks
		if(checkCycles()){
			//Resolve Deadlock
			removeWFG(0);
		}else{
			dl_list.clear();
		}
		
	}
	
	private static boolean checkCycles(){
		//Finds all the cycles and updates the deadlocked T/P list
		
		boolean cycle=false;
		
		//Maintain a cycle List that is basically a do not follow list
		ArrayList<CycleNode> cycleList=new ArrayList<CycleNode>();
		
		for(int iter=0;iter<wfg.size();iter++){
			Node n1=wfg.get(iter);
				
			for(int c=0;c<n1.waitsFor.size();c++){
				int tr=n1.waitsFor.get(c);
				
				ArrayList<Integer> tr_list=new ArrayList<Integer>();
				
				tr_list.add(n1.trans);
				
				CycleNode cNode=new CycleNode();
				cNode.tid=tr;
				cNode.goto_trans=tr_list;
				cycleList.add(cNode);
			}
			
		}
		
		//Check cycleList for cycles
		for(int iter=0;iter<cycleList.size();iter++){
			
			//Exception are thrown when a transaction of the number corresponding to that index does not exist
			//So we just skip over them
			CycleNode cNode=cycleList.get(iter);
			
			for(int c=0;c<cNode.goto_trans.size();c++){
				int goto_trans=cNode.goto_trans.get(c);
				
				try{
					ArrayList<Integer> goto_list=new ArrayList<Integer>();
					for(int x=0;x<cycleList.size();x++){
						if(cycleList.get(x).tid==goto_trans){
							goto_list=cycleList.get(x).goto_trans;
							break;
						}
					}
					
					if(goto_list.contains(cNode.tid)){
						cycle=true;
						boolean do_not_add=false;
						for(int x=0;x<dl_list.size();x++){
							if(dl_list.get(x)==cNode.tid){
								do_not_add=true;
							}
						}
						if(!do_not_add){
							dl_list.add(cNode.tid);
						}
						
					}
				}catch(Exception e){
					//Do Nothing
				}
			}
		}
		
		return cycle;
	}

	/***
	 * The argument indicates from where the removeWFG function was called.
	 * If called from insertWFG, set reason = 0, it is because of Deadlock Removal.
	 * If called from schedule, set reason = Transaction/Process ID, called when an commit or abort operation has to take place.
	 */
	private static void removeWFG(int reason){
		
		//Commit or Abort Took place and WFG has to updated.
		if(reason!=0){
			for(int iter=0;iter<wfg.size();iter++){
				if(wfg.get(iter).trans==reason){
					wfg.remove(iter);
					break;
				}
			}
		}else{ //Deadlock Managing
			
			//Select the T/P to Abort Arbitrarily
			Random gen=new Random();
			
			int lenth=dl_list.size();
			int select=gen.nextInt(lenth);
				
			int kill=dl_list.get(select);
				
			for(int iter=0;iter<wfg.size();iter++){
				if(wfg.get(iter).trans==kill){
					
					//Remove form deadlock list
					dl_list.remove(select);
					
					//Remove from WFG
					wfg.remove(iter);
					
					//Remove Operation list
					for(int x=0;x<op_list.size();x++){
						if(op_list.get(x).id==kill){
							op_list.remove(x);
							break;
						}
					}
					
					//Remove tp_list
					for(int x=0;x<tp_list.size();x++){
						if(tp_list.get(x).id==kill){
							tp_list.remove(x);
							break;
						}
					}
					
					//Remove Locks
					remove_locks(kill);
					
					//Perform Unblocking
					for(int x=0;x<dl_list.size();x++){
						int id=dl_list.get(x);
						//Find the Script that id belongs to
						for(int y=0;y<tp_list.size();y++){
							if(tp_list.get(y).id==id){
								//Find Index of ArrayList By ScriptName to block
								for(int z=0;z<Initialize.scriptNames.size();z++){
									if(Initialize.scriptNames.get(z).contentEquals(tp_list.get(y).script)){
										Initialize.blockList.set(z, -1);
									}
								}
							}
						}
					}
					
					dl_list.clear();
					break;
				}
			}
		}
		
	}
	
	//The parameters are type (0,1) for process or transaction, 
		//id is taken from globalID that is unique ID given for a transaction/process
		//Query as in (R X 10), script is for script file name 
	public static boolean LockTable(int type, int id, String query, String script){
			
			//boolean value to decide whether to allow the locks or not
			boolean allow = true;
			//integer that counts the number of conflicts
			int conflicts = 0;
			//Splitting the query into 3 parts (query examples-- (R X 1532) , (G X 412), I X (123,Sam,412-519-4288) )
			//query_parts[0] has the operations -- I,G,M.... 
			//query-parts[1] has table_name
			//query_parts[2] has PK or Area code or tuple according to the operation 
			String[] query_parts = query.split(query);
			
			//making new node
			LockNode locknode = new LockNode();
			locknode.blocktype = type;
			locknode.transId =id;
			locknode.lockType = query_parts[0];
			locknode.tableName = query_parts[1];
			
			//initialize variables
			String key_table = query_parts[1];
			String key_pk = "";
			String key_ac = "";

			//clearing the conflicts list every time for new query
			conflict_Transactions.clear();
			
			//Finding out which locks are required and
			//check the hash maps whether there is a lock present on the required PK or AC or Table
			
			if (locknode.lockType == new String("I")){
				key_pk = query_parts[0];
				String[] temp = query_parts[2].split(",");
				//getting the area code
				key_ac = temp[2].split("-")[0];
				
				//If there is a lock on the table
				if (Table_Locks.containsKey(key_table)){
					List<LockNode> nodelist = Table_Locks.get((locknode.tableName)); 
					Iterator<LockNode> table_iterator = nodelist.iterator();
					while (table_iterator.hasNext()) {
						LockNode matchnode = table_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;
						//checking if the same transaction/process already has lock on the table
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}
						//calling compatibility table to check for conflicts
						if (id != matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						table_iterator.next();
					}
				}
				
				//If there is a lock on the pk
				if (PK_Locks.containsKey(key_pk)){
					List<LockNode> nodelist = PK_Locks.get(key_pk); 
					Iterator<LockNode> pk_iterator = nodelist.iterator();
					while (pk_iterator.hasNext()) {
						LockNode matchnode = pk_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;
						
						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}

						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						pk_iterator.next();
					}
				}
				//if there is a lock on the ac
				if (AC_Locks.containsKey(key_ac)){
					List<LockNode> nodelist = AC_Locks.get(key_ac); 
					Iterator<LockNode> ac_iterator = nodelist.iterator();
					while (ac_iterator.hasNext()) {
						LockNode matchnode = ac_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;
						
						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}
						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						ac_iterator.next();
					}
				}
			}
			if (locknode.lockType ==  new String("R")){
				key_pk = query_parts[0];
				
				//If there is a lock on the table
				if (Table_Locks.containsKey(key_table)){
					List<LockNode> nodelist = Table_Locks.get((locknode.tableName)); 
					Iterator<LockNode> table_iterator = nodelist.iterator();
					while (table_iterator.hasNext()) {
						LockNode matchnode = table_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;
						
						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}
						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						table_iterator.next();
					}
				}
				
				//If there is a lock on the pk
				if (PK_Locks.containsKey(key_pk)){
					List<LockNode> nodelist = PK_Locks.get(key_pk); 
					Iterator<LockNode> pk_iterator = nodelist.iterator();
					while (pk_iterator.hasNext()) {
						LockNode matchnode = pk_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;

						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}

						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						pk_iterator.next();
					}
				}
			}
			if (locknode.lockType == new String("G") || locknode.lockType ==  new String("M")){
				String[] temp = query_parts[2].split(",");
				key_ac = temp[2].split("-")[0];
				
				//If there is a lock on the table
				if (Table_Locks.containsKey(key_table)){
					List<LockNode> nodelist = Table_Locks.get((locknode.tableName)); 
					Iterator<LockNode> table_iterator = nodelist.iterator();
					while (table_iterator.hasNext()) {
						LockNode matchnode = table_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;

						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}

						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						table_iterator.next();
					}
				}
				
				//if there is a lock on the ac
				if (AC_Locks.containsKey(key_ac)){
					List<LockNode> nodelist = AC_Locks.get(key_ac); 
					Iterator<LockNode> ac_iterator = nodelist.iterator();
					while (ac_iterator.hasNext()) {
						LockNode matchnode = ac_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;

						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}

						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
							
						}
						ac_iterator.next();
					}
				}
			}
			if (locknode.lockType == new String("D")){
				key_table = query_parts[1];
				
				//If there is a lock on the table
				if (Table_Locks.containsKey(key_table)){
					List<LockNode> nodelist = Table_Locks.get((locknode.tableName)); 
					Iterator<LockNode> table_iterator = nodelist.iterator();
					while (table_iterator.hasNext()) {
						LockNode matchnode = table_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;

						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}

						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						table_iterator.next();
					}
				}
				
				//If there is a lock on the pk from that table
				if (PK_Locks.containsKey(key_pk)){
					List<LockNode> nodelist = PK_Locks.get(key_pk); 
					Iterator<LockNode> pk_iterator = nodelist.iterator();
					while (pk_iterator.hasNext()) {
						LockNode matchnode = pk_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;

						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}

						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						pk_iterator.next();
					}
				}
				
				//if there is a lock on the ac from that table
				if (AC_Locks.containsKey(key_ac)){
					List<LockNode> nodelist = AC_Locks.get(key_ac); 
					Iterator<LockNode> ac_iterator = nodelist.iterator();
					while (ac_iterator.hasNext()) {
						LockNode matchnode = ac_iterator.next();
						String oldType = matchnode.lockType;
						int oldTransOrProc = matchnode.blocktype;

						//checking if the same transaction/process already has lock on the item					
						if (locknode.transId == matchnode.transId)
						{
								continue;
						}

						//calling compatibility table to check for conflicts
						if (id!= matchnode.transId){
							if( chkCompTbl(locknode.lockType, oldType, locknode.blocktype, oldTransOrProc) )
							{
								conflicts++;
								conflict_Transactions.add(matchnode.transId);
							}
						}
						ac_iterator.next();
					}
				}
			}

			//If no conflict then add the locks		
			if(conflicts != 0){
				allow = false;
				logger.info("Transaction with id "+locknode.transId+" has conflicts with the following transactions: ");
				for (int i=0;i<conflicts;i++){
					//updating logger 
					logger.info(" "+conflict_Transactions.get(0));
				}
			}
			if(allow){
				if (locknode.lockType == new String("I")){
					//adding node in PK_hashmap
					List<LockNode> pk_nodelist = PK_Locks.get(key_pk);
					//if there is no record of that key in the hash map
					if (pk_nodelist.size() == 0){
						pk_nodelist.add(locknode);
						PK_Locks.put(key_pk, pk_nodelist);
						//updating logger 
						logger.info("Transaction "+locknode.transId+ "got the I lock on "+key_pk);
					}
					else{
						LockNode lastnode = pk_nodelist.get(pk_nodelist.size() - 1);
						lastnode.next_ptr = locknode;					
					}
					
					//adding node in AC_hashmap
					List<LockNode> ac_nodelist = AC_Locks.get(key_ac);
					//if there is no record of that key in the hash map
					if (ac_nodelist.size() == 0){
						ac_nodelist.add(locknode);
						AC_Locks.put(key_ac, ac_nodelist);
					}
					else{
						LockNode lastnode = ac_nodelist.get(ac_nodelist.size() - 1);
						lastnode.next_ptr = locknode;					
					}

				}
				if (locknode.lockType == new String("R")){
					//adding node in PK_hashmap
					List<LockNode> pk_nodelist = PK_Locks.get(key_pk);
					//if there is no record of that key in the hash map
					if (pk_nodelist.size() == 0){
						pk_nodelist.add(locknode);
						PK_Locks.put(key_pk, pk_nodelist);
						//updating logger 
						logger.info("Transaction "+locknode.transId+ "got the R lock on "+key_pk);
					}
					else{
						LockNode lastnode = pk_nodelist.get(pk_nodelist.size() - 1);
						lastnode.next_ptr = locknode;					
					}				
				}
				if (locknode.lockType == new String("G") || locknode.lockType == new String("M")){
					//adding node in AC_hashmap
					List<LockNode> ac_nodelist = AC_Locks.get(key_ac);
					//if there is no record of that key in the hash map
					if (ac_nodelist.size() == 0){
						ac_nodelist.add(locknode);
						AC_Locks.put(key_ac, ac_nodelist);
						//updating logger 
						logger.info("Transaction "+locknode.transId+ "got the G/M lock on "+key_ac);
					}
					else{
						LockNode lastnode = ac_nodelist.get(ac_nodelist.size() - 1);
						lastnode.next_ptr = locknode;					
					}
				}
				if (locknode.lockType == new String("D")){
					//adding node in Table_hashmap
					List<LockNode> table_nodelist = Table_Locks.get(key_table);
					//if there is no record of that key in the hash map
					if (table_nodelist.size() == 0){
						table_nodelist.add(locknode);
						Table_Locks.put(key_table, table_nodelist);
						//updating logger 
						logger.info("Transaction "+locknode.transId+ "got the D lock on "+key_table);
					}
					else{
						LockNode lastnode = table_nodelist.get(table_nodelist.size() - 1);
						lastnode.next_ptr = locknode;					
					}
				}
			}
			//returns flag whether locks added or not
			return allow;
		}
		//Implementation of lock tables ends here
		
		
		// compatibility table
		// order is: It, Rt, Gt, Mt, Dt, Ip, Rp, Gp, Mp, Dp
		// It means "I (transaction)", Ip means "I (process)"
		
	public static boolean[][] compTable = 
			{
			 	{false, false, false, false, false, false , true , true , true , false }, 
			 	{false, true , true , true , false, false , true , true , true , false },
			 	{false, true , true , true , false, false , true , true , true , false },
			 	{false, true , true , true , false, false , true , true , true , false },
			 	{false, false, false, false, false, false , true , true , true , false },
			 	
			 	{false, false, false, false, false , false , true , true , true , false },
			 	{false, true , true , true , false , false , true , true , true , false },
			 	{false, true , true , true , false , false , true , true , true , false },
			 	{false, true , true , true , false , false , true , true , true , false },
			 	{false, false, false, false, false , false , true , true , true , false }
			};
		
	//for removing locks of aborted transaction
	public static void remove_locks(int transId){

		//checking PK_Locks hash map for the locks by any aborted transaction 
		for(int i=0;i<PK_Locks.size();i++){
			//getting all the keys in the hash map
			Set<String> keys = PK_Locks.keySet();
			Iterator<String> keys_iterator = keys.iterator();
			String key="";
			while(keys_iterator.hasNext()){
				//getting the key
				key = keys_iterator.next();
				//getting the value of the key i.e, list of lock nodes
				List<LockNode> nodelist = PK_Locks.get(key);
				Iterator<LockNode> table_iterator = nodelist.iterator();
				
				int index = 0;				 
				while (table_iterator.hasNext()) {
					LockNode pknode = table_iterator.next();
					//deleting the nodes holding the lock by the aborted transaction
					if(pknode.transId == transId){
						nodelist.remove(index);
					}
					index++;
				}
				//if the values list of a key is empty delete the key
				if(PK_Locks.get(key)==null){
					PK_Locks.remove(key);
				}
			}

		}
		
		//checking AC_Locks hash map for the locks by any aborted transaction 
			for(int i=0;i<AC_Locks.size();i++){
				//getting all the keys in the hash map
				Set<String> keys = AC_Locks.keySet();
				Iterator<String> keys_iterator = keys.iterator();
				String key="";
				while(keys_iterator.hasNext()){
					//getting the key
					key = keys_iterator.next();
					//getting the value of the key i.e, list of lock nodes
					List<LockNode> nodelist = AC_Locks.get(key);
					Iterator<LockNode> ac_iterator = nodelist.iterator();
					
					int index = 0;				 
					while (ac_iterator.hasNext()) {
						LockNode ac_node = ac_iterator.next();
						//deleting the nodes holding the lock by the aborted transaction
						if(ac_node.transId == transId){
							nodelist.remove(index);
						}
						index++;
					}
					//if the values list of a key is empty delete the key
					if(AC_Locks.get(key)==null){
						AC_Locks.remove(key);
					}
				}

			}

			//checking Table_Locks hash map for the locks by any aborted transaction 
			for(int i=0;i<Table_Locks.size();i++){
				//getting all the keys in the hash map
				Set<String> keys = Table_Locks.keySet();
				Iterator<String> keys_iterator = keys.iterator();
				String key="";
				while(keys_iterator.hasNext()){
					//getting the key
					key = keys_iterator.next();
					//getting the value of the key i.e, list of lock nodes
					List<LockNode> nodelist = Table_Locks.get(key);
					Iterator<LockNode> table_iterator = nodelist.iterator();
					
					int index = 0;				 
					while (table_iterator.hasNext()) {
						LockNode table_node = table_iterator.next();
						//deleting the nodes holding the lock by the aborted transaction
						if(table_node.transId == transId){
							nodelist.remove(index);
						}
						index++;
					}
					//if the values list of a key is empty delete the key
					if(Table_Locks.get(key)==null){
						Table_Locks.remove(key);
					}
				}

			}

	}

	/* A List containing the 5 operators. This list is used to get the index of 
		 * operators in the compatibility table.
		 */
		
		public final static ArrayList<String> operators = new ArrayList<String>();
		static {
			operators.add("I");
			operators.add("R");
			operators.add("G");
			operators.add("M");
			operators.add("D");
		}
		
		
		public static boolean chkCompTbl(String newType, String oldType, int newTransOrProc, int oldTransOrProc) {
			
			/* 
			 * newType is the operator in the new read line, for example,
			 * the new read line is (R X 10), the newType is R.
			 * oldType is the operator that holds a lock towards a tuple.
			 * for example, (I X 10) is a command(Process) we read before 
			 * and kept in lock-table, now the new transaction command 
			 * (R X 10) reach, we found that the new command probably have 
			 * conflict with (I X 10), so the newType is R, newTransOrProc 
			 * is 1, oldType is I, oldTransOrProc is 0.
			 */
			
			int oldTypeIndex, newTypeIndex;
			oldTypeIndex = operators.indexOf(oldType);
			newTypeIndex = operators.indexOf(newType);
			if(oldTransOrProc == 0) oldTypeIndex += 5;
			if(newTransOrProc == 0) newTypeIndex += 5;
			
			return compTable[oldTypeIndex][newTypeIndex]; 
		}
	
	private static class Node{
		private int trans;
		private ArrayList<Integer> waitsFor=new ArrayList<Integer>();
		
		Node(int t,ArrayList<Integer> p){
			trans=t;
			waitsFor=p;
		}
	}
	
	private static class Begin{
		private int id;
		private int type;
		private String script;
		
	}

	private static class OpNode{
		private int id;
		private ArrayList<String> op=new ArrayList<String>();
	}

	private static class CycleNode{
		private int tid;
		private ArrayList<Integer> goto_trans=new ArrayList<Integer>();
	}
	
	//Lock node contains the details of block type - 0 for process and 1 for transaction,.... 
	public static class LockNode {
		int blocktype;
		int transId;
		String tableName;
		String lockType;
		LockNode next_ptr;
	}

}
