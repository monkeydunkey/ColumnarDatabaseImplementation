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
                    System.out.println(split.length);
                    query.run(split);
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
            //SystemDefs.JavabaseBM.flushAllPages();
            SystemDefs.JavabaseDB.closeDB();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}
