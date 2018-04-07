package tests;

import diskmgr.pcounter;

public class BitMapJoinCommand {
    public static void run(String[] split) {
        pcounter.initialize();

        //todo parse input params

        //todo execute

        System.out.println("BitMapJoin done!");
        System.out.println("Disk read count: "+ pcounter.rcounter);
        System.out.println("Disk write count: "+ pcounter.wcounter );
    }
}
