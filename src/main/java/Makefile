#This Makefile is used to create assignments.

#the following macros should be updated according to
#the assignment to be generated

JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class\
         heap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class\
				 BigT/*.class driver/*.class storage/*.class

JDKPATH = $(JAVA_HOME)
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

DOCFILES=bufmgr diskmgr global chainexception heap btree iterator index

##############  update the above for each assignment in making

ASSIGN=.:..
LIBDIR=$(ASSIGN)/lib
KEY=$(ASSIGN)/key
SRC=$(ASSIGN)/src

IMAGELINK=$(PACKAGEINDEX)/images
PACKAGEINDEX=$(ASSIGN)/javadoc

JAVADOC=javadoc -public -d $(PACKAGEINDEX)

### Generate jar and javadoc files.  Apply to most assignments.
db: 
	make -C global
	make -C chainexception
	make -C btree
	make -C bufmgr
	make -C diskmgr
	make -C heap
	make -C index
	make -C iterator
	make -C BigT
	make -C driver
	make -C storage

doc:
	$(JAVADOC) $(DOCFILES)

test: db
	cd tests; make run

stream: db
	cd tests; make runstream

testbigt: db
	cd tests; make runbigt

testvstream: db
	cd tests; make runvstream
	
testsort: db
	cd tests; make runsorttest

testsmallmap: db
	cd tests; make runsmallmap

testsmallmapfile: db
	cd tests; make runsmallmapfile

filestream: db
	cd tests; make filestream

batchinsert: db
	cd driver; make batchinsert

driver: db
	cd driver; make driver

clean:
	\rm -f $(CLASSPATH)/*.class *~ \#* core $(JARFILES) TRACE

