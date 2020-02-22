JDKPATH = $(JAVA_HOME)
LIBPATH = .:..

CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -g -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

map:Map.java
	$(JAVAC) Map.java

stream:Stream.java
	$(JAVAC) Stream.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
