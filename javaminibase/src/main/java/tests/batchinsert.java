package tests;

import diskmgr.ColumnDB;

import diskmgr.DB;
import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import btree.BT;
import btree.BTreeFile;
import columnar.*;
import java.io.*;
import java.util.Arrays;
import java.util.StringTokenizer;

import diskmgr.pcounter;

public class batchinsert {

	public static void run(String[] argv)
	{	
		pcounter.initialize();
		
		String filepath = "./";
		String DATA_FILE_NAME = argv[0];
		String COLUMN_DB_NAME = argv[1];
		String COLUMNAR_FILE_NAME = argv[2];
		String NUM_OF_COLUMNS = argv[3];

		int batch_insert = 1;
		System.out.println("DATAFILENAME: " + DATA_FILE_NAME);
		System.out.println("COLUMNDBNAME: " + COLUMN_DB_NAME);
		System.out.println("COLUMNARFILENAME: " + COLUMNAR_FILE_NAME);
		System.out.println("NUMCOLUMNS: " + NUM_OF_COLUMNS);

		int numcolumns = Integer.parseInt(NUM_OF_COLUMNS);

		AttrType[] type = new AttrType[numcolumns];
		String[] columnnames = new String[numcolumns];

		SystemDefs sysdef = null;
	    try {
			//SystemDefs sysdef = new SystemDefs(COLUMN_DB_NAME,100000,100,"Clock");
			//Path dbpath = Paths.get(filepath + COLUMN_DB_NAME);

			File db_file = new File(COLUMN_DB_NAME);
			if(db_file.exists()) {	// file found
				System.out.printf("An existing database (%s) was found, opening database.\n", COLUMN_DB_NAME);
				// open database with 100 buffers
				sysdef = new SystemDefs(COLUMN_DB_NAME,0,100000,"Clock");
			}else
			{
				System.out.println("Opening new DB: " + COLUMN_DB_NAME);
				sysdef = new SystemDefs(COLUMN_DB_NAME,100000,100000,"Clock");
			}

			//Borrowed tokenizer
			FileInputStream fin = new FileInputStream(filepath+ DATA_FILE_NAME);
			DataInputStream din = new DataInputStream(fin);
			BufferedReader bin = new BufferedReader(new InputStreamReader(din));
			
			String line = bin.readLine();
		
			StringTokenizer st = new StringTokenizer(line);
			int i = 0;
			int tuplelength = 0;

			while(st.hasMoreTokens())	
			{	
				String argToken = st.nextToken();
				StringTokenizer temp = new StringTokenizer(argToken);
				
				String tokenname = temp.nextToken(":");
				//System.out.println(tokenname);
				String tokentype = temp.nextToken(":");
				//System.out.println(tokentype);

				columnnames[i] = tokenname;
				
				if (tokentype.equals("int"))
					//Parse int attribute
				{
					type[i] = new AttrType(AttrType.attrInteger);
					tuplelength = tuplelength + 4;
				}
				else {
					//Parse string(length) attribute
					type[i] = new AttrType(AttrType.attrString);
					
					StringTokenizer temp1 = new StringTokenizer(tokentype);
					
					temp1.nextToken("(");
					// dummy is the "25)" part
					String dummy = temp1.nextToken("(");
					temp1 = null; 
					temp1 =	new StringTokenizer(dummy);
					String stringlength = temp1.nextToken(")"); 
					Size.STRINGSIZE = Integer.parseInt(stringlength);
					tuplelength = tuplelength + Size.STRINGSIZE;
				}
				i++;		
			}
			
			//Create a new columnarfile
			/*
			if db exists then open it

			else setup a new file
			*/
			Columnarfile cf;
			System.out.println("Page Read and Write Counts before Starting");
			System.out.println("Disk read count: "+ pcounter.rcounter);
			System.out.println("Disk write count: "+ pcounter.wcounter );
			try {
				//checking if the file exists
				System.out.println("Table Name: " + COLUMNAR_FILE_NAME + "aa");
				cf = new Columnarfile (COLUMNAR_FILE_NAME);
			} catch (Exception ex){
				// null pointer exception the file does not exists so creating a new one
				cf = new Columnarfile (COLUMNAR_FILE_NAME, numcolumns, type, columnnames);
			}
			if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
				System.out.println("*** Columnar File initialization left unpinned pages\n");
			}
			//cf.setColumnNames(columnnames);
			System.out.println("Page Read and Write Counts after Columnar File Initialization");
			System.out.println("Disk read count: "+ pcounter.rcounter);
			System.out.println("Disk write count: "+ pcounter.wcounter );

			System.out.println("Inserting tuples START");
			//Putting in records
			int insertCount = 0;
			byte [] tupledata = new byte[tuplelength];
			int offset = 0;

			while((line = bin.readLine()) != null)
			{
				StringTokenizer columnvalue = new StringTokenizer (line);
				
				for(AttrType attr: type)
				{
					String column = columnvalue.nextToken();
					if(attr.attrType == AttrType.attrInteger)
					{
						Convert.setIntValue(Integer.parseInt(column), offset, tupledata);
						offset = offset + 4;
					}
					else if (attr.attrType == AttrType.attrString)
					{
						Convert.setStrValue(column, offset, tupledata);
						offset = offset + Size.STRINGSIZE;
					}
				}
				cf.insertTuple(tupledata, batch_insert);
				offset = 0;
				insertCount++;
				if (insertCount%100 == 0){
					System.out.printf("Page Read and Write Counts after (%d) inserts\n",insertCount );
					System.out.println("Disk read count: "+ pcounter.rcounter);
					System.out.println("Disk write count: "+ pcounter.wcounter );
					Arrays.fill(tupledata, (byte)0);
				}

			}
			if (SystemDefs.JavabaseBM.getNumUnpinnedBuffers() != SystemDefs.JavabaseBM.getNumBuffers()) {
				System.out.println("*** Insert left unpinned pages\n");
			}
			SystemDefs.JavabaseBM.resetAllPins();
			SystemDefs.JavabaseBM.flushAllPages();
			SystemDefs.JavabaseDB.closeDB();
			System.out.println("Insertion done!");
			System.out.println("Disk read count: "+ pcounter.rcounter);
			System.out.println("Disk write count: "+ pcounter.wcounter );
		} 

		catch (Exception e){
			e.printStackTrace();
		}
	}

}