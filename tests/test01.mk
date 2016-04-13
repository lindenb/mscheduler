all: jeter1.txt jeter2.txt
	echo "Hello world"

jeter1.txt: jeter3.txt
	touch $@
jeter3.txt:
	touch $@
jeter2.txt:
	ls > $@
