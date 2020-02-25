JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

sourcefiles = $(wildcard $(CURDIR)/*.java)

classfiles = $(patsubst %.java, %.class, $(sourcefiles))

all: $(classfiles)

$(CURDIR)/%.class: $(CURDIR)/%.java
	$(JAVAC) $<

clean:
	\rm -f *.class *~ \#* core 
