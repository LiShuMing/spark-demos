
#TODO
#export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.7.0_51.jdk/Contents/Home

build:
	mvn clean package

clean:
	mvn clean
	rm -rf spark-demo-*
