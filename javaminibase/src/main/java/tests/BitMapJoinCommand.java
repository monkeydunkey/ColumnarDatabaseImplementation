package tests;

import diskmgr.pcounter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BitMapJoinCommand {
    public static void run(String[] split) {
        pcounter.initialize();
        //bmj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST EQUICONST [TARGETCOLUMNS] NUMBUF

        final String regex = "bmj (\\w+) (\\w+) (\\w+) (\\{.+?\\}) (\\{.+?\\}) (\\{.+?\\}) (\\[.+?\\])\\s(.+$)";
        final String inputString = String.join(" ", split);

        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(inputString);

        while (matcher.find()) {
            System.out.println("Full match: " + matcher.group(0));
            for (int i = 1; i <= matcher.groupCount(); i++) {
                System.out.println("Group " + i + ": " + matcher.group(i));
            }
        }


        //todo parse input params

        //todo execute

        System.out.println("BitMapJoin done!");
        System.out.println("Disk read count: "+ pcounter.rcounter);
        System.out.println("Disk write count: "+ pcounter.wcounter );
    }
}
