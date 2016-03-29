lib.dir?=j4make/lib

apache.derby.tools  = \
	$(lib.dir)/org/apache/derby/derby/10.12.1.1/derby-10.12.1.1.jar \
	$(lib.dir)/org/apache/derby/derbytools/10.12.1.1/derbytools-10.12.1.1.jar

all_maven_jars = $(sort  ${apache.derby.tools})

all: ${all_maven_jars}

j4make/dist/j4make.jar :
	(cd j4make && ${MAKE})
	
	



${apache.derby.tools}  : 
	mkdir -p $(dir $@) && wget -O "$@" "http://central.maven.org/maven2/$(patsubst ${lib.dir}/%,%,$@)"

