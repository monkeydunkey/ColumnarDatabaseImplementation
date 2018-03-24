package tests;

import btree.*;
import bufmgr.*;
import global.*;
import heap.*;
import columnar.*;
import bitmap.*;
import java.io.*;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import diskmgr.pcounter;
import java.util.Scanner;

public class TestPrompt {
    public static void menu()
    {
        System.out.println("---------------------------- MENU ----------------------------");
        System.out.println("Format of command line input: \n");

        System.out.println("Batch Insert:");
        System.out.println("batchinsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS");
        System.out.println("DATAFILENAME should be a .txt file in the .../src/tests folder\n");

        System.out.println("Index:");
        System.out.println("index COLUMNDBNAME COLUMNARFILENAME COLUMNNAME INDEXTYPE");
        System.out.println("INDEXTYPE valid inputs: BTREE or BITMAP\n");

        System.out.println("Query:");
        System.out.println("query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE");
        System.out.println("ACCESSTYPE valid inputs: FILESCAN, COLUMNSCAN, BTREE, or BITMAP\n");

        System.out.println("Delete Query:");
        System.out.println("query COLUMNDBNAME COLUMNARFILENAME [TARGETCOLUMNNAMES] VALUECONSTRAINT NUMBUF ACCESSTYPE PURGED");
        System.out.println("ACCESSTYPE valid inputs: FILESCAN, COLUMNSCAN, BTREE, or BITMAP");
        System.out.println("PURGED valid inputs: 0 -> don't purge deleted tuples, 1 -> purge deleted tuples\n");

        System.out.println("Quit Program Execution:");
        System.out.println("quit");

        System.out.println("------------------------- EXIT MENU --------------------------");
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

            if( command == "batchinsert" )
            {
                try {
                    batchinsert.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command == "index" )
            {
                try {
                    index.run(split);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            else if( command == "query" )
            {
                //Uncomment when query is finished
//                try {
//                    query.run(split);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
            else if( command == "delete_query" )
            {
                //Uncomment when delete_query is finished
//                try {
//                    delete_query.run(split);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }
            else if( command == "quit" )
            {
                quit = true;
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
