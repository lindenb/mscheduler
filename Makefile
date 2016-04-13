lib.dir?=j4make/lib
EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
berkeleydb.version?=6.2.31
berkeleydb.jar?=$(lib.dir)/com/sleepycat/je/${berkeleydb.version}/je-${berkeleydb.version}.jar

define call_compile

$(1): ${all_maven_jars} $${berkeleydb.jar} j4make/dist/j4make.jar
	rm -rf _tmp  dist/$(1).jar
	mkdir -p _tmp/META-INF dist
	$$(foreach J,$$(filter %.jar,$$^),unzip -q -o $${J} -d _tmp;)
	javac -d _tmp -sourcepath src/main/java \
		-cp  "$$(subst $$(SPACE),:,$$(filter %.jar,$$^))" \
		./src/main/java/com/github/lindenb/mscheduler/$(2).java
	echo "Manifest-Version: 1.0" > _tmp/META-INF/MANIFEST.MF
	echo "Main-Class: com.github.lindenb.mscheduler.$(2)" >>_tmp/META-INF/MANIFEST.MF
	jar cMf dist/$(1).jar -C _tmp .
	rm -rf _tmp

endef

.PHONY: all clean test sgescheduler ccrtscheduler

j4make.jars =  \
	$(lib.dir)/commons-cli/commons-cli/1.3.1/commons-cli-1.3.1.jar \
	$(lib.dir)/org/slf4j/slf4j-api/1.7.13/slf4j-api-1.7.13.jar \
	$(lib.dir)/org/slf4j/slf4j-simple/1.7.13/slf4j-simple-1.7.13.jar \
	$(lib.dir)/commons-codec/commons-codec/1.10/commons-codec-1.10.jar


all_maven_jars = $(sort  ${apache.derby.tools} ${j4make.jars})

all: sgescheduler ccrtscheduler


$(eval $(call call_compile,sgescheduler,SGEScheduler))
$(eval $(call call_compile,ccrtscheduler,CCRTScheduler))


test : sgescheduler
	rm -rf _tmp
	mkdir -p _tmp
	java -jar  dist/sgescheduler.jar build -d ${PWD}/_tmp -m ${PWD}/tests/test01.mk
	java -jar  dist/sgescheduler.jar list -d ${PWD}/_tmp
	java -jar  dist/sgescheduler.jar run -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar list -d ${PWD}/_tmp
	java -jar  dist/sgescheduler.jar run  -j 3 -d ${PWD}/_tmp
	java -jar  dist/sgescheduler.jar run  -j 3 -d ${PWD}/_tmp
	java -jar  dist/sgescheduler.jar run  -j 3 -d ${PWD}/_tmp
	java -jar  dist/sgescheduler.jar run  -j 3 -d ${PWD}/_tmp
	java -jar  dist/sgescheduler.jar list -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar list -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar run -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar run -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar run -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar run -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar run -d ${PWD}/_tmp
	sleep 5
	java -jar  dist/sgescheduler.jar list -d ${PWD}/_tmp
	

j4make/dist/j4make.jar :
	(cd j4make && ${MAKE})
	
${berkeleydb.jar}:
	mkdir -p $(dir $@) && wget -O "$@" "http://download.oracle.com/maven/com/sleepycat/je/${berkeleydb.version}/je-${berkeleydb.version}.jar"


${all_maven_jars}  : 
	mkdir -p $(dir $@) && wget -O "$@" "http://central.maven.org/maven2/$(patsubst ${lib.dir}/%,%,$@)"

clean:
	rm -rf ${lib.dir} _tmp
