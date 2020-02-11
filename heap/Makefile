JDKPATH = C:/Program Files/Java/jdk1.8.0_144
LIBPATH = C:/Users/cranm/OneDrive/Documents/CSE510/minjava/minjava/javaminibase/src
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

hf:Heapfile.java
	$(JAVAC) Heapfile.java

hp:HFPage.java
	$(JAVAC) HFPage.java

tuple:Tuple.java
	$(JAVAC) Tuple.java

scan:Scan.java
	$(JAVAC) Scan.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
