package tests;

import columnar.Columnarfile;
import iterator.FldSpec;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BitMapJoinInput {
    //bmj COLUMNDB OUTERFILE INNERFILE OUTERCONST INNERCONST EQUICONST [TARGETCOLUMNS] NUMBUF

    private String columnDB;
    private String outerFile;
    private String innerFile;
    private String outerConst;
    private String innerConst;
    private String equiConst;
    private String targetColumns;
    private String numbuf;
    private int leftJoinFieldIndex;
    private int rightJoinFieldIndex;

    public int getLeftJoinFieldIndex() {
        return leftJoinFieldIndex;
    }

    public void setLeftJoinFieldIndex(int leftJoinFieldIndex) {
        this.leftJoinFieldIndex = leftJoinFieldIndex;
    }

    public int getRightJoinFieldIndex() {
        return rightJoinFieldIndex;
    }

    public void setRightJoinFieldIndex(int rightJoinFieldIndex) {
        this.rightJoinFieldIndex = rightJoinFieldIndex;
    }

    public String getColumnDB() {
        return columnDB;
    }

    public void setColumnDB(String columnDB) {
        this.columnDB = columnDB;
    }

    public String getOuterFile() {
        return outerFile;
    }

    public void setOuterFile(String outerFile) {
        this.outerFile = outerFile;
    }

    public String getInnerFile() {
        return innerFile;
    }

    public void setInnerFile(String innerFile) {
        this.innerFile = innerFile;
    }

    public String getOuterConst() {
        return outerConst;
    }

    public void setOuterConst(String outerConst) {
        this.outerConst = outerConst;
    }

    public String getInnerConst() {
        return innerConst;
    }

    public void setInnerConst(String innerConst) {
        this.innerConst = innerConst;
    }

    public String getEquiConst() {
        return equiConst;
    }

    public void setEquiConst(String equiConst) {
        this.equiConst = equiConst;
    }

    public String getTargetColumns() {
        return targetColumns;
    }

    public void setTargetColumns(String targetColumns) {
        this.targetColumns = targetColumns;
    }

    public String getNumbuf() {
        return numbuf;
    }

    public void setNumbuf(String numbuf) {
        this.numbuf = numbuf;
    }

    public static BitMapJoinInput parse(String[] split) {
        final String regex = "(\\w+) (\\w+) (\\w+) \\{(.+?)\\} \\{(.+?)\\} \\{(.+?)\\} \\[(.+?)\\]\\s(.+$)";
        final String inputString = String.join(" ", split);

        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(inputString);

        BitMapJoinInput bitMapJoinInput = new BitMapJoinInput();
        while (matcher.find()) {
            bitMapJoinInput.setColumnDB(matcher.group(1));
            bitMapJoinInput.setOuterFile(matcher.group(2));
            bitMapJoinInput.setInnerFile(matcher.group(3));
            bitMapJoinInput.setOuterConst(matcher.group(4));
            bitMapJoinInput.setInnerConst(matcher.group(5));
            bitMapJoinInput.setEquiConst(matcher.group(6));
            bitMapJoinInput.setTargetColumns(matcher.group(7));
            bitMapJoinInput.setNumbuf(matcher.group(8));
        }

        return bitMapJoinInput;
    }

    public static String getRegexMatch(String regex, String input, int group){
        final Pattern pattern = Pattern.compile(regex);
        final Matcher matcher = pattern.matcher(input);

        while (matcher.find()) {
            return matcher.group(group);
        }

        throw new RuntimeException("No Regex Match");
    }

    public String getT1ColumnName(String input){
        return getRegexMatch("T1\\.(.)", input, 1);
    }
    public String getT2ColumnName(String input){
        return getRegexMatch("T2\\.(.)", input, 1);
    }

    public FldSpec[] getProjectionList() {
        return new FldSpec[0];//todo
    }

    public int getNumbufInt() {
        return Integer.parseInt(getNumbuf());
    }

    public int parseLeftJoinFieldIndex(Columnarfile innerColumnarFile) {
        String equiConst = getEquiConst();
        equiConst = equiConst.replace(" ", "");

        // seperate statments
        // statment AND statment OR statment
        // For bitmap statment can only be = or !=
        // todo handle ors and ands

        return innerColumnarFile.getColumnIndexByName(getT1ColumnName(equiConst));
    }

    public int parseRightJoinFieldIndex(Columnarfile outerColumnarFile) {
        String equiConst = getEquiConst();
        equiConst = equiConst.replace(" ", "");

        // seperate statments
        // statment AND statment OR statment
        // For bitmap statment can only be = or !=
        // todo handle ors and ands

        return outerColumnarFile.getColumnIndexByName(getT2ColumnName(equiConst));
    }

    public void finishParsing(Columnarfile outerColumnarFile, Columnarfile innerColumnarFile){
        setLeftJoinFieldIndex(parseLeftJoinFieldIndex(innerColumnarFile));
        setRightJoinFieldIndex(parseRightJoinFieldIndex(outerColumnarFile));
    }
}
