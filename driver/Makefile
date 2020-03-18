# modify the paths appropriately
JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)
DATA  = $(CURDIR)/data/*.csv -classpath $(CLASSPATH)

sourcefiles = $(wildcard $(CURDIR)/*.java)
#sourcefiles = $(wildcard $(CURDIR)/*java)
classfiles = $(patsubst %.java, %.class, $(sourcefiles))

all: $(classfiles)

$(CURDIR)/%.class: $(CURDIR)/%.java
	$(JAVAC) $<

batchinsert: $(CURDIR)/BatchInsert.class
	$(JAVA) driver.BatchInsert

#query: $(CURDIR)/Query.class
#	$(JAVA) driver.Query

clean:
	\rm -f $(CURDIR)/*.class *~ \#* core $(JARFILES) TRACE

