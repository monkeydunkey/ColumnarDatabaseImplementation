package tests;

import diskmgr.ColumnDB;

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
import java.nio.file.*;

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

		System.out.println("DATAFILENAME: " + DATA_FILE_NAME);
		System.out.println("COLUMNDBNAME: " + COLUMN_DB_NAME);
		System.out.println("COLUMNARFILENAME: " + COLUMNAR_FILE_NAME);
		System.out.println("NUMCOLUMNS: " + NUM_OF_COLUMNS);

		int numcolumns = Integer.parseInt(NUM_OF_COLUMNS);

		AttrType[] type = new AttrType[numcolumns];
		String[] columnnames = new String[numcolumns];

    
	    try {
			//SystemDefs sysdef = new SystemDefs(COLUMN_DB_NAME,100000,100,"Clock");
			Path dbpath = Paths.get(filepath + COLUMN_DB_NAME);

			if (Files.exists(dbpath)) {
				System.out.println("Opening existing DB: " + COLUMN_DB_NAME);
				SystemDefs.JavabaseDB.openDB(COLUMN_DB_NAME);
			}
			else
			{
				System.out.println("Opening new DB: " + COLUMN_DB_NAME);
				SystemDefs sysdef = new SystemDefs(COLUMN_DB_NAME,100000,100,"Clock");
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
			Columnarfile cf = new Columnarfile (COLUMNAR_FILE_NAME, numcolumns, type, columnnames);
			//cf.setColumnNames(columnnames);


			System.out.println("Inserting tuples START");
			//Putting in records
			
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
				cf.insertTuple(tupledata);
				offset = 0;
			
				Arrays.fill(tupledata, (byte)0);
			}

			System.out.println("Insertion done!");
			System.out.println("Disk read count: "+ pcounter.rcounter);
			System.out.println("Disk write count: "+ pcounter.wcounter );
		} 

		catch (Exception e){
			e.printStackTrace();
		}
	}

}