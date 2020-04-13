# modify the paths appropriately
JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)
DATA  = $(CURDIR)/data/project2_testdata.csv -classpath $(CLASSPATH)

sourcefiles = $(filter-out \
	$(CURDIR)/TestDriver.java, $(wildcard $(CURDIR)/*.java))
#sourcefiles = $(wildcard $(CURDIR)/*java)
classfiles = $(patsubst %.java, %.class, $(sourcefiles))

all: $(classfiles)

$(CURDIR)/TestDriver.class: $(CURDIR)/TestDriver.java
	$(JAVAC) $(CURDIR)/TestDriver.java

$(CURDIR)/%.class: $(CURDIR)/%.java $(CURDIR)/TestDriver.class
	$(JAVAC) $(CURDIR)/TestDriver.java $<

run: all
	$(JAVA) tests.SM_JoinTest
	$(JAVA) tests.SortTest
	$(JAVA) tests.JoinTest
	$(JAVA) tests.IndexTest
	$(JAVA) tests.BTTest
	$(JAVA) tests.HFTest
	$(JAVA) tests.DBTest
	$(JAVA) tests.BMTest
	$(JAVA) -ea tests.StreamTest
	$(JAVA) tests.bigtTest
	$(JAVA) tests.VStreamTest
	$(JAVA) tests.FileStreamTest
	$(JAVA) -ea tests.SmallMapTest
	$(JAVA) -ea tests.SmallMapFileTest
	$(JAVA) -ea tests.SmallMapFilePrimeTest

runstream: $(CURDIR)/StreamTest.class
	$(JAVA) -ea tests.StreamTest

runbigt: $(CURDIR)/bigtTest.class
	$(JAVA) tests.bigtTest

runvstream: $(CURDIR)/VStreamTest.class
	$(JAVA) tests.VStreamTest

runsorttest: $(CURDIR)/SortTest.class
	$(JAVA) tests.SortTest

filestream: $(CURDIR)/FileStreamTest.class
	$(JAVA) tests.FileStreamTest

smallmap: $(CURDIR)/SmallMapTest.class
	$(JAVA) tests.SmallMapTest

smallmapfile: $(CURDIR)/SmallMapFileTest.class
	$(JAVA) tests.SmallMapFileTest

smallmapfile: $(CURDIR)/SmallMapFilePrimeTest.class
	$(JAVA) tests.SmallMapFilePrimeTest

clean:
	\rm -f $(CURDIR)/*.class *~ \#* core $(JARFILES) TRACE

