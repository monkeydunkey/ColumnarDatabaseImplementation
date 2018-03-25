package tests;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import columnar.*;
import bitmap.*;
import java.io.*;
import java.util.*;

import diskmgr.pcounter;

import static global.GlobalConst.NUMBUF;

public class TestPrompt {

    private static void menu()
    {
        System.out.println("\n---------------------------------------------- MENU ----------------------------------------------");
        System.out.println("Format of command line input:");

        System.out.println("**********Batch Insert**********");
        System.out.println("> batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
        System.out.println("DATAFILENAME should be a .txt file in the .../src/tests folder");

        System.out.println("*************Index**************");
        System.out.println("> index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE");
        System.out.println("INDEXTYPE valid inputs: BTREE or BITMAP");

        System.out.println("*************Query**************");
        System.out.println("> query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE");
        System.out.println("ACCESSTYPE valid inputs: FILESCAN, COLUMNSCAN, BTREE, or BITMAP");

        System.out.println("**********Delete Query**********");
        System.out.println("> delete_query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE PURGED");
        System.out.println("ACCESSTYPE valid inputs: FILESCAN, COLUMNSCAN, BTREE, or BITMAP");
        System.out.println("PURGED valid inputs: 0 -> don't purge deleted tuples, 1 -> purge deleted tuples");

        System.out.println("Quit Program Execution:");
        System.out.println("> quit");

        System.out.println("--------------------------------------------- EXIT MENU --------------------------------------------");
        System.out.print("> ");
    }

    public static void main( String[] args )
    {
        boolean quit = false;
        do
        {
            menu();
            Scanner scan = new Scanner(System.in);
            String invoke = scan.nextLine();
            String[] split = invoke.split("\\s+"); // Whitespace delimiter to split up command line invocation
            String command = split[0]; // Save command before mutating the command array

            // Remove the program name from the arguments to pass into programs (not simple as arrays are fixed size; arraylist must be used)
            List<String> list = new ArrayList<String>(Arrays.asList(split));
            list.remove(0);
            split = list.toArray(split);

            if( command.equals("batchinsert") )
            {
                try {
                    batchinsert.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command.equals("index") )
            {
                try {
                    Index.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command.equals("query") )
            {
                //Uncomment when query is finished
//                try {
//                    query.run(split);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
            else if( command.equals("delete_query") )
            {
                //Uncomment when delete_query is finished
//                try {
//                    delete_query.run(split);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
            else if( command.equals("quit") )
            {
                quit = true;
                scan.close();
            }
            else
            {
                System.out.println( "ERROR - Invalid input! Please try again!\n" );
            }
        } while( !quit );
        System.out.println("Exiting....\n");
        // Done with database operations!
        try {
            SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

//    public static void runBatchInsert(String[] argv)
//    {
//        pcounter.initialize();
//
//        String filepath = "./";
//        String DATA_FILE_NAME = argv[0];
//        String COLUMN_DB_NAME = argv[1];
//        String COLUMNAR_FILE_NAME = argv[2];
//        String NUM_OF_COLUMNS = argv[3];
//
//        System.out.println("DATAFILENAME: " + DATA_FILE_NAME);
//        System.out.println("COLUMNDBNAME: " + COLUMN_DB_NAME);
//        System.out.println("COLUMNARFILENAME: " + COLUMNAR_FILE_NAME);
//        System.out.println("NUMCOLUMNS: " + NUM_OF_COLUMNS);
//
//        int numcolumns = Integer.parseInt(NUM_OF_COLUMNS);
//
//        AttrType[] type = new AttrType[numcolumns];
//        String[] columnnames = new String[numcolumns];
//
//
//        try {
//            SystemDefs sysdef = new SystemDefs(COLUMN_DB_NAME,100000,100,"Clock");
//
//            //Borrowed tokenizer
//            FileInputStream fin = new FileInputStream(filepath+ DATA_FILE_NAME);
//            DataInputStream din = new DataInputStream(fin);
//            BufferedReader bin = new BufferedReader(new InputStreamReader(din));
//
//            String line = bin.readLine();
//
//            StringTokenizer st = new StringTokenizer(line);
//            int i = 0;
//            int tuplelength = 0;
//
//            while(st.hasMoreTokens())
//            {
//                String argToken = st.nextToken();
//                StringTokenizer temp = new StringTokenizer(argToken);
//
//                String tokenname = temp.nextToken(":");
//                String tokentype = temp.nextToken(":");
//
//                columnnames[i] = tokenname;
//
//                if (tokentype.equals("int"))
//                //Parse int attribute
//                {
//                    type[i] = new AttrType(AttrType.attrInteger);
//                    tuplelength = tuplelength + 4;
//                }
//                else {
//                    //Parse string(length) attribute
//                    type[i] = new AttrType(AttrType.attrString);
//
//                    StringTokenizer temp1 = new StringTokenizer(tokentype);
//
//                    temp1.nextToken("(");
//                    // dummy is the "25)" part
//                    String dummy = temp1.nextToken("(");
//                    temp1 = null;
//                    temp1 =	new StringTokenizer(dummy);
//                    String stringlength = temp1.nextToken(")");
//                    Size.STRINGSIZE = Integer.parseInt(stringlength);
//                    tuplelength = tuplelength + Size.STRINGSIZE;
//                }
//                i++;
//            }
//
//            //Create a new columnarfile
//			/*
//			if db exists then open it
//
//			else setup a new file
//			*/
//            Columnarfile cf = new Columnarfile (COLUMNAR_FILE_NAME, numcolumns, type);
//            cf.setColumnNames(columnnames);
//
//
//            System.out.println("Inserting tuples START");
//            //Putting in records
//
//            byte [] tupledata = new byte[tuplelength];
//            int offset = 0;
//
//            while((line = bin.readLine()) != null)
//            {
//                StringTokenizer columnvalue = new StringTokenizer (line);
//
//                for(AttrType attr: type)
//                {
//                    String column = columnvalue.nextToken();
//                    if(attr.attrType == AttrType.attrInteger)
//                    {
//                        Convert.setIntValue(Integer.parseInt(column), offset, tupledata);
//                        offset = offset + 4;
//                    }
//                    else if (attr.attrType == AttrType.attrString)
//                    {
//                        Convert.setStrValue(column, offset, tupledata);
//                        offset = offset + Size.STRINGSIZE;
//                    }
//                }
//                cf.insertTuple(tupledata);
//                offset = 0;
//
//                Arrays.fill(tupledata, (byte)0);
//            }
//
//            System.out.println("Insertion done!");
//            System.out.println("Disk read count: "+ pcounter.rcounter);
//            System.out.println("Disk write count: "+ pcounter.wcounter );
//        }
//
//        catch (Exception e){
//            e.printStackTrace();
//        }
//    }
//
//    public static void runIndex( String[] args ) throws HFDiskMgrException, InvalidTupleSizeException, HFException, InvalidSlotNumberException, SpaceNotAvailableException, HFBufMgrException, IOException, AddFileEntryException, GetFileEntryException, ConstructPageException {
//        pcounter.initialize(); // Initializes read & write counters to 0
//
//        final String COLUMN_DB_NAME = args[0];
//        final String COLUMNAR_FILE_NAME = args[1];
//        final String COLUMN_NAME = args[2];
//        final String INDEX_TYPE = args[3];
//
//        /* Uncomment for debugging */
//         String filePath = "./";
//         System.out.println( "COLUMNDBNAME: " + COLUMN_DB_NAME );
//         System.out.println( "COLUMNARFILE: " + COLUMNAR_FILE_NAME );
//         System.out.println( "COLUMNNAME: " + COLUMN_NAME );
//         System.out.println( "INDEXTYPE: " + INDEX_TYPE );
//
//        System.out.println( "Running index test...\n" );
//
//        // TODO - Implement rest of program
//        /*
//         * Logic
//         * Use COLUMN_DB_NAME to lookup the database (e.g.colDB1)
//         * Use COLUMNAR_FILE_NAME to look up columnar file
//         * Use COLUMN_NAME to lookup attribute
//         * Use INDEX_TYPE to select how column in indexed
//         */
//
//        //SystemDefs sysdef = new SystemDefs( COLUMN_DB_NAME, NUMBUF+20, NUMBUF, "Clock" );
//
//        Columnarfile cFile = null;
//
//        try {
//            // Retrieve the Columnarfile using the provided string
//            cFile = new Columnarfile(COLUMNAR_FILE_NAME);
//        }
//        catch(IOException e) {
//            e.printStackTrace();
//        }
//
//        boolean success = false;
//
//        if( INDEX_TYPE.equals("BTREE"))
//        {
//            // todo convert COLUMN_NAME to int value
//            // success = cFile.createBTreeIndex(COLUMN_NAME);
//            success = true;
//
//        }
//        else if( INDEX_TYPE.equals("BITMAP") )
//        {
//            // todo determine the type (ValueClass) to send as parameter 2
//            //success = cFile.createBitMapIndex(cFile.getColumnIndexByName(COLUMN_NAME), cFile.getColumnTypeByName(COLUMN_NAME));
//        }
//        else
//        {
//            System.out.println("Error - INDEXTYPE should be either BTREE or BITMAP!");
//        }
//        if(!success)
//            System.out.println("Error - Could not create index!");
//        else
//            System.out.println("SUCCESS - Index created!");
//
//        System.out.println("Index test finished!\n");
//        System.out.println("Disk read count: " + pcounter.rcounter);
//        System.out.println("Disk write count: " + pcounter.wcounter);
//    }
}
