# modify the paths appropriately
JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

MapTest:MapTest.java
	$(JAVAC) TestDriver.java MapTest.java

# test buffer manager

BMTest:BMTest.java
	$(JAVAC) TestDriver.java BMTest.java

# test disk manager

DBTest:DBTest.java
	$(JAVAC) TestDriver.java DBTest.java

# test heapfile 

HFTest:HFTest.java
	$(JAVAC) TestDriver.java HFTest.java

# test btree

BTTest:BTTest.java
	$(JAVAC) TestDriver.java BTTest.java

# test index

IndexTest:IndexTest.java
	$(JAVAC) TestDriver.java IndexTest.java

# test join

JoinTest:JoinTest.java
	$(JAVAC) TestDriver.java JoinTest.java

# test sort

SortTest:SortTest.java
	$(JAVAC) TestDriver.java SortTest.java

# test sortMerge

SMJoin:SM_JoinTest.java
	$(JAVAC) SM_JoinTest.java TestDriver.java

run: SMJoin SortTest JoinTest IndexTest BTTest HFTest DBTest BMTest MapTest
	$(JAVA) tests.SM_JoinTest
	$(JAVA) tests.SortTest
	$(JAVA) tests.JoinTest
	$(JAVA) tests.IndexTest
	$(JAVA) tests.BTTest
	$(JAVA) tests.HFTest
	$(JAVA) tests.DBTest
	$(JAVA) tests.BMTest
	$(JAVA) tests.MapTest
