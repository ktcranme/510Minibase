# modify the paths appropriately
JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

sourcefiles = $(wildcard $(CURDIR)/*.java)

classfiles = $(patsubst %.java, %.class, $(sourcefiles))

all: $(classfiles)

$(CURDIR)/%.class: $(CURDIR)/%.java
	$(JAVAC) $<

run: all
	$(JAVA) tests.SM_JoinTest
	$(JAVA) tests.SortTest
	$(JAVA) tests.JoinTest
	$(JAVA) tests.IndexTest
	$(JAVA) tests.BTTest
	$(JAVA) tests.HFTest
	$(JAVA) tests.DBTest
	$(JAVA) tests.BMTest
	$(JAVA) tests.MapTest
