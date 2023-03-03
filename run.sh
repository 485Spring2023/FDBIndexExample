OUT_DIR=out
javac -cp ".:lib/*" -d ${OUT_DIR} Employee.java FDBHelper.java ClusteredHashIndexRecord.java NonClusteredHashIndexRecord.java HashIndexes.java SimpleIndexExample.java
java -cp ".:lib/*:${OUT_DIR}" SimpleIndexExample