# modify the paths appropriately
JDKPATH = /usr/lib/jvm/java-8-oracle
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

# test columnar index scan
CMIndexTest:CMTest.java
	$(JAVAC) TestDriver.java CMIndexTest.java

cmindextest: CMIndexTest
	$(JAVA) tests.CMIndexTest

# test columnar file
CMTest:CMTest.java
        $(JAVAC) TestDriver.java CMTest.java

cmtest: CMTest
        $(JAVA) tests.CMTest

# test columnar nested loop join
ColumnarJoinTest:ColumnarJoinTest.java
        $(JAVAC) TestDriver.java ColumnarJoinTest.java

columnarjointest: ColumnarJoinTest
        $(JAVA) tests.ColumnarJoinTest

# test buffer manager

BMTest:BMTest.java
	$(JAVAC) TestDriver.java BMTest.java

bmtest: BMTest
	$(JAVA) tests.BMTest

# test disk manager

DBTest:DBTest.java
	$(JAVAC) TestDriver.java DBTest.java

dbtest: DBTest
	$(JAVA) tests.DBTest

# test heapfile 

HFTest:HFTest.java
	$(JAVAC) TestDriver.java HFTest.java

hftest: HFTest
	$(JAVA) tests.HFTest

# test btree

BTTest:BTTest.java
	$(JAVAC) TestDriver.java BTTest.java

bttest: BTTest
	$(JAVA) tests.BTTest

# test index

IndexTest:IndexTest.java
	$(JAVAC) TestDriver.java IndexTest.java

indextest: IndexTest
	$(JAVA) tests.IndexTest

# test join

JoinTest:JoinTest.java
	$(JAVAC) TestDriver.java JoinTest.java

jointest: JoinTest
	$(JAVA) tests.JoinTest

# test sort

SortTest:SortTest.java
	$(JAVAC) TestDriver.java SortTest.java

sorttest: SortTest
	$(JAVA) tests.SortTest

# test sortMerge

SMJoin:SM_JoinTest.java
	$(JAVAC) SM_JoinTest.java TestDriver.java

sortmerge: SMJoin
	$(JAVA) tests.SM_JoinTest

# test test prompt

TestPrompt:TestPrompt.java
	$(JAVAC) TestPrompt.java

testprompt: TestPrompt
	$(JAVA) tests.TestPrompt
