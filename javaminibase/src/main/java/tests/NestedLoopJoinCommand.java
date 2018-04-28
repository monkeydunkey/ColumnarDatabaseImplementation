package tests;

import diskmgr.pcounter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Arrays;

public class NestedLoopJoinCommand {
    public static void run(String[] split) {
        pcounter.initialize();
        String dbName = "";
        String OuterFileName = "";
        String InnerFileName = "";
        String[] OuterConstraints;
        String[] InnerConstraints;
        String[] JoinConstraints;
        String OuterAccType = "";
        String[] TargetColumns;
        String NumBuf = "";

        //Function Signature :
        //nlj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST JOINCONST OUTERACCTYPE [TARGETCOLUMNS] NUMBUF

        //final String regex = "(\\w+) (\\w+) (\\w+) (\\{.+?\\}) (\\{.+?\\}) (\\{.+?\\}) (\\w+) (\\[.+?\\])\\s(.+$)";
        final String regex = "(\\w+) (\\w+) (\\w+) \\{((\\w+ (=|!=|>|<) \\w+(( AND | OR ))?)*)\\} \\{((\\w+ (=|!=|>|<) \\w+(( AND | OR ))?)*)\\} \\{((\\w+\\.\\w+ (=|!=|>|<) \\w+\\.\\w+(( AND | OR ))?)*)\\} \\[((\\w+\\.\\w+ ?)*)\\] (\\d+)";

        final String inputString = String.join(" ", split);
        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(inputString);
        String[] parsedArr = new String[22];
        int parsedArr_count = 0;

        while (matcher.find()) {
            System.out.println("Full match: " + matcher.group(0));
            for (int i = 1; i <= matcher.groupCount(); i++) {

                parsedArr[parsedArr_count] = matcher.group(i);
                parsedArr_count++;
            }
        }

        //Setting variables from given query
        dbName = parsedArr[0];
        InnerFileName = parsedArr[1];
        OuterFileName = parsedArr[2];
        OuterConstraints = parsedArr[3].split("\\s+");
        InnerConstraints = parsedArr[4].split("\\s+");
        JoinConstraints = parsedArr[5].split("\\s+");
        OuterAccType = parsedArr[6];
        TargetColumns = parsedArr[7].split("\\s+");
        NumBuf = parsedArr[8];

        System.out.println(Arrays.toString(OuterConstraints));

        System.out.println("NestedLoopJoin done!");
        System.out.println("Disk read count: "+ pcounter.rcounter);
        System.out.println("Disk write count: "+ pcounter.wcounter );
    }
}
