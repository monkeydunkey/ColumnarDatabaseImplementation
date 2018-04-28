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

        System.out.println("**********Nested Loop Join Query**********");
        System.out.println("> nlj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST JOINCONST OUTERACCTYPE [TARGETCOLUMNS] NUMBUF");

        System.out.println("Quit Program Execution:");
        System.out.println("> quit");

        System.out.println("--------------------------------------------- EXIT MENU --------------------------------------------");
        System.out.print("> ");
    }

    public static void main( String[] args )
    {
        boolean quit = false;
        SystemDefs sysdef = new SystemDefs("db1",100000,100,"Clock");
        System.out.println("Setting up temporary DB: db1");
        boolean testMode = false;

        Scanner scan;
        if(args.length == 0){
            scan = new Scanner(System.in);
        }else{
            // the below is for testing purposes (instead of type each command from the commandline, we can conveniently read it from a file
            // this allows us to put different sample inputs into many files and quickly iterate through them to test
            // while keeping the behavior of the program the same)
            try {
                testMode = true;
                scan = new Scanner(new File(args[0]));
            } catch (FileNotFoundException e) {
                System.out.println("File not found: "+args[0]);
                return;
            }
        }

        do
        {
            if(!testMode) {
                menu();
            }
            String invoke = scan.nextLine();
            System.out.println("input: "+invoke);
            String[] splitInit = invoke.split("\\s+"); // Whitespace delimiter to split up command line invocation
            String command = splitInit[0]; // Save command before mutating the command array
            // Remove the program name from the arguments to pass into programs (not simple as arrays are fixed size; arraylist must be used)
            List<String> list = new ArrayList<String>(Arrays.asList(splitInit));
            list.remove(0);
            String[] split = new String[list.size()];
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
                try {
                    query_v2.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command.equals("delete_query") )
            {
                try {
                    delete.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command.equals("bmj") )
            {
                try {
                    BitMapJoinCommand.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command.equals("nlj") )
            {
                try {
                    NestedLoopJoinCommand.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command.equals("quit") )
            {
                quit = true;
                scan.close();
                try{
                    SystemDefs.JavabaseDB.DBDestroy();
                } catch (IOException ex){
                    System.out.println("could not destroy the db");
                }
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
}
