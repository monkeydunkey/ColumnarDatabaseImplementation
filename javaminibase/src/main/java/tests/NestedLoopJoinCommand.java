package tests;

import diskmgr.pcounter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NestedLoopJoinCommand {
    public static void run(String[] split) {
        pcounter.initialize();
        String dbName = "";
        String OuterFileName = "";
        String InnerFileName = "";

        //Function Signature :
        //nlj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST JOINCONST OUTERACCTYPE [TARGETCOLUMNS] NUMBUF

        String text    =
                //"nlj column_db_name columnar_file_name_b columnar_file_name_a {A = 1,A = 2} {B = 1,B = 2} {T1.A = T2.B} test [T1.A T1.B T2.B] 50";
"column_db_name columnar_file_name_b columnar_file_name_a {A = 1,A = 2} {B = 1,B = 2} {T1.A = T2.B} test [T1.A T1.B T2.B] 50";
        final String regex = "(\\w+) (\\w+) (\\w+) (\\{.+?\\}) (\\{.+?\\}) (\\{.+?\\}) (\\w+) (\\[.+?\\])\\s(.+$)";

        final String inputString = String.join(" ", split);
        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(inputString);
        String[] parsedArr = new String[9];
        int parsedArr_count = 0;

        while (matcher.find()) {
            System.out.println("Full match: " + matcher.group(0));
            for (int i = 1; i <= matcher.groupCount(); i++) {

                parsedArr[parsedArr_count] = matcher.group(i);
                parsedArr_count++;
            }
        }

        dbName = parsedArr[0] // Setting COLUMNDBNAME
        InnerFileName = parsedArr[1] // Setting INNERFILENAME
        OuterFileName = parsedArr[2] //Setting OUTERFILENAME
        

        System.out.println("NestedLoopJoin done!");
        System.out.println("Disk read count: "+ pcounter.rcounter);
        System.out.println("Disk write count: "+ pcounter.wcounter );
    }
}
