package tests;

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

public class BatchInsert extends TestDriver implements GlobalConst{

private int TRUE  = 1;
private int FALSE = 0;
private boolean OK = true;
private boolean FAIL = false;
	
	public static void main (String argv[])
	{	
		
		
		int readcount = 0, writecount = 0; 

		String filepath = "./";
		System.out.println("DATAFILENAME: " + argv[0]);
		System.out.println("COLUMNDBNAME: " + argv[1]);
		System.out.println("COLUMNARFILENAME: " + argv[2]);
		System.out.println("NUMCOLUMNS: " + argv[3]);

		int numcolumns = Integer.parseInt(argv[3]);

		AttrType[] type = new AttrType[numcolumns];

		System.out.print ("\n" + "Running " + testName() + " tests...." + "\n");
    
	    try {
	      SystemDefs sysdef = new SystemDefs( dbpath, NUMBUF+20, NUMBUF, "Clock" );
	    }
	    
	    catch (Exception e) {
	      Runtime.getRuntime().exit(1);
	    }
		
		try {
			//Borrowed tokenizer
			FileInputStream fin = new FileInputStream(filepath+argv[0]);
			DataInputStream din = new DataInputStream(fin);
			BufferedReader bin = new BufferedReader(new InputStreamReader(din));
			
			readcount = pcounter.rcounter;
			writecount = pcounter.wcounter;
			
			String line = bin.readLine();
		
			StringTokenizer st = new StringTokenizer(line);
			int i = 0, tuplelength = 0;

			while(st.hasMoreTokens())	
			{	
				String argToken = st.nextToken();
				StringTokenizer temp = new StringTokenizer(argToken);
				
				String tokenname = temp.nextToken(":");
				String tokentype = temp.nextToken(":");
				
				
				if (tokentype.equals("int"))
					//Parse int attribute
				{
					type[i] = new AttrType(AttrType.attrInteger);
					tuplelength = tuplelength + 4;
				}
				else {
					//Parse string(length) attribute
					type[i] = new AttrType(AttrType.attrString);
					
					StringTokenizer temp = new StringTokenizer(tokentype);
					
					temp1.nextToken("(");
					String dummy = temp.nextToken("(");
					temp = null; 
					temp =	new StringTokenizer(dummy);
					String dummy1 = temp.nextToken(")"); 
					Size.STRINGSIZE = Integer.parseInt(dummy1);

					System.out.println("size: "+ Size.STRINGSIZE);
					tuplelength = tuplelength + Size.STRINGSIZE;
				}
				i++;		
			}
			
			//Create a new columnarfile
			/*
			if db exists then open it

			else setup a new file
			*/
			Columnarfile cf = new Columnarfile (argv[2], numcolumns, type);
			


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
			System.out.println("Disk read count: "+(pcounter.rcounter - readcount));
			System.out.println("Disk write count: "+(pcounter.wcounter - writecount));
			
		} 

		catch (Exception e){
			e.printStackTrace();
		}
	}
}