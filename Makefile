lib.dir?=j4make/lib
EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
berkeleydb.version?=6.2.31
berkeleydb.jar?=$(lib.dir)/com/sleepycat/je/${berkeleydb.version}/je-${berkeleydb.version}.jar


j4make.jars =  \
	$(lib.dir)/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar \
	$(lib.dir)/org/slf4j/slf4j-api/1.7.13/slf4j-api-1.7.13.jar \
	$(lib.dir)/org/slf4j/slf4j-simple/1.7.13/slf4j-simple-1.7.13.jar \
	$(lib.dir)/commons-codec/commons-codec/1.10/commons-codec-1.10.jar


all_maven_jars = $(sort  ${apache.derby.tools} ${j4make.jars})

all: sgescheduler  ${all_maven_jars} ${berkeleydb.jar} j4make/dist/j4make.jar
	rm -rf _tmp
	mkdir _tmp
	java -cp "$(subst $(SPACE),:,$(filter %.jar,$^)):dist/sgescheduler.jar"  com.github.lindenb.mscheduler.SGEScheduler \
		 build -m trace.txt -d _tmp
	java -cp "$(subst $(SPACE),:,$(filter %.jar,$^)):dist/sgescheduler.jar"  com.github.lindenb.mscheduler.SGEScheduler \
		 run  -d _tmp


sgescheduler: ${all_maven_jars} ${berkeleydb.jar} j4make/dist/j4make.jar
	rm -rf _tmp
	mkdir -p _tmp/META-INF dist
	javac -d _tmp -sourcepath src/main/java \
		-cp  "$(subst $(SPACE),:,$(filter %.jar,$^))" \
		./src/main/java/com/github/lindenb/mscheduler/SGEScheduler.java
	jar cvf dist/mscheduler.jar -C _tmp .
	rm -rf _tmp
	

j4make/dist/j4make.jar :
	(cd j4make && ${MAKE})
	
	

${berkeleydb.jar}:
	mkdir -p $(dir $@) && wget -O "$@" "http://download.oracle.com/maven/com/sleepycat/je/${berkeleydb.version}/je-${berkeleydb.version}.jar"


${all_maven_jars}  : 
	mkdir -p $(dir $@) && wget -O "$@" "http://central.maven.org/maven2/$(patsubst ${lib.dir}/%,%,$@)"

clean:
	rm -rf ${lib.dir}
