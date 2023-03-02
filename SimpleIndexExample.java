import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;

import java.util.ArrayList;
import java.util.List;

public class SimpleIndexExample {

    // dataset
    // Employee
    static Employee[] employees = new Employee[] {
        new Employee(1L, "Bob", 1000L, 1L),
        new Employee(2L, "Shuqin", 1200L, 2L),
        new Employee(3L, "Alice", 8000L, 3L)
    };

    public static void addAttributeValuePairToTable(Transaction tx, DirectorySubspace table,
            long primaryKey, String attributeName, Object attributeValue) {
        Tuple keyTuple = new Tuple();
        keyTuple = keyTuple.add(primaryKey).add(attributeName);

        Tuple valueTuple = new Tuple();
        valueTuple = valueTuple.addObject(attributeValue);
        tx.set(table.pack(keyTuple), valueTuple.pack());
    }

    public static Employee queryEmployeeTableWithPrimaryKey(Database db, DirectorySubspace table, long primaryKey) {
        Transaction tx = db.createTransaction();

        List<String> paths = table.getPath();
        System.out.print("Query Table [" + paths.get(paths.size() - 1) + "] with primary key " + primaryKey + ":");

        Employee employee = new Employee();
        try {
            Tuple SSN = Tuple.fromBytes(tx.get(table.pack(new Tuple().add(primaryKey).add(Employee.EMPLOYEE_ATTRIBUTE_SSN))).get());
            employee.setSSN((Long) SSN.get(0));
            Tuple Name = Tuple.fromBytes(tx.get(table.pack(new Tuple().add(primaryKey).add(Employee.EMPLOYEE_ATTRIBUTE_NAME))).get());
            employee.setName((String) Name.get(0));
            Tuple Salary = Tuple.fromBytes(tx.get(table.pack(new Tuple().add(primaryKey).add(Employee.EMPLOYEE_ATTRIBUTE_SALARY))).get());
            employee.setSalary((Long) Salary.get(0));
            Tuple Dno = Tuple.fromBytes(tx.get(table.pack(new Tuple().add(primaryKey).add(Employee.EMPLOYEE_ATTRIBUTE_DNO))).get());
            employee.setDno((Long) Dno.get(0));
        } catch (Exception e) {
            System.out.println("ERROR when querying the data: " + e);
            e.printStackTrace();
        }

        return employee;
    }

    public static void main(String[] args) {
        FDB fdb = FDB.selectAPIVersion(710);
        Database db = null;
        DirectorySubspace rootDirectory = null;

        boolean isUsingClusterHashIndex = true;
        try {
            db = fdb.open();
        } catch (Exception e) {
            System.out.println("ERROR: the database is not successfully opened: " + e);
        }


        System.out.println("Open FDB Successfully!");

        FDBHelper.clear(db);
        System.out.println("Clear FDB Successfully!");

        // initialize root directory, which stands for the Company
        try {
            rootDirectory = DirectoryLayer.getDefault().createOrOpen(db,
                    PathUtil.from("Company")).join();
        } catch (Exception e) {
            System.out.println("ERROR: the root directory is not successfully opened: " + e);
        }
        System.out.println("Create Company directory successfully!");

        final DirectorySubspace employeeTable = rootDirectory.createOrOpen(db, PathUtil.from(Employee.EMPLOYEE_TABLENAME)).join();

        System.out.println("Create Employee directory successfully!");

        String targetAttrValue = "Bob";
        String targetAttrToBuildIndex = Employee.EMPLOYEE_ATTRIBUTE_NAME;
        Long targetAttrHashValue = HashIndexes.nameHashMap.get(targetAttrValue);

        if (isUsingClusterHashIndex) {
            // using cluster hash index
            System.out.println("Build Cluster Hash Index on Employee Table, Attribute: [" + targetAttrToBuildIndex + "], search for value \"" + targetAttrValue +"\"");
            List<ClusterHashIndexRecord> clusterHashIndexRecords = HashIndexes.buildClusterHashIndex(employees, targetAttrToBuildIndex);

            // persist the index records to FDB
            Transaction tx = FDBHelper.openTransaction(db);
            for (ClusterHashIndexRecord record : clusterHashIndexRecords) {
                FDBHelper.putKVPair(tx, employeeTable, record.getKeyTuple(), record.getValueTuple());
            }
            FDBHelper.commitTransaction(tx);

            // get all the key-value pairs with prefix(Employee, NonClusterHashIndex, Name, HashVal)
            Tuple queryPrefix = ClusterHashIndexRecord.getPrefixQueryTuple(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, targetAttrHashValue);
            List<KeyValue> indexKVPairs = FDBHelper.getPrefixKVPairs(db, employeeTable, queryPrefix);

            Employee e = new Employee();
            boolean isFirstKV = true;
            boolean found = false;
            long currentPrimaryKey = 0;

            // for each key-value pair
            for (KeyValue kv : indexKVPairs) {
                Tuple key = employeeTable.unpack(kv.getKey());
                Tuple val = Tuple.fromBytes(kv.getValue());

                // extract the primary key, attribute name from the key; attribute value from the val
                long primaryKey = (long) ClusterHashIndexRecord.getPrimaryKeyFromKeyTuple(key);
                String attrName = (String) ClusterHashIndexRecord.getAttributeNameFromKeyTuple(key);
                Object attrVal = val.get(0);


                if (isFirstKV) {
                    currentPrimaryKey = primaryKey;
                    isFirstKV = false;
                } else if (currentPrimaryKey != primaryKey) {
                    if (found) {
                        break;
                    }
                    e = new Employee();
                }

                e.setAttributeValue(attrName, attrVal);
                // if the attribute name of this kv pair is also the attribute that has index structure on it
                if (attrName.equals(targetAttrToBuildIndex)) {
                    // if the value equals to the target value, means we found it
                    if (targetAttrValue.equals(attrVal)) {
                        // we found it!
                        found = true;
                    }
                }
            }

            if (!found) {
                System.out.println("Target value: " + targetAttrValue + " of Attribute [" + targetAttrToBuildIndex + "] not found");
            } else {
                System.out.println(e);
            }
        } else {
            // using non-cluster hash index
            System.out.println("Build Non-Cluster Hash Index on Employee Table, Attribute: [" + targetAttrToBuildIndex + "], search for value \"" + targetAttrValue +"\"");

            // load employee dataset first
            for (Employee e : employees) {
                Transaction insertionTx = db.createTransaction();
                long ssn = e.getSSN();
                addAttributeValuePairToTable(insertionTx, employeeTable,
                    ssn, Employee.EMPLOYEE_ATTRIBUTE_SSN, e.getSSN());
                addAttributeValuePairToTable(insertionTx, employeeTable,
                    ssn, Employee.EMPLOYEE_ATTRIBUTE_NAME, e.getName());
                addAttributeValuePairToTable(insertionTx, employeeTable,
                    ssn, Employee.EMPLOYEE_ATTRIBUTE_SALARY, e.getSalary());
                addAttributeValuePairToTable(insertionTx, employeeTable,
                    ssn, Employee.EMPLOYEE_ATTRIBUTE_DNO, e.getDno());
                insertionTx.commit().join();
            }
            System.out.println("Finish initialization of Employee Table.");

            // generate the non-cluster hash index records
            List<NonClusterHashIndexRecord> nonClusterHashIndexRecords = HashIndexes.buildNonClusterHashIndex(employees, targetAttrToBuildIndex);
            Transaction tx = FDBHelper.openTransaction(db);
            // persist the non-cluster hash index to FDB
            for (NonClusterHashIndexRecord record : nonClusterHashIndexRecords) {
                FDBHelper.putKVPair(tx, employeeTable, record.getKeyTuple(), record.getValueTuple());
            }
            FDBHelper.commitTransaction(tx);

            // get all the key-value pairs with prefix(Employee, NonClusterHashIndex, Name, HashVal)
            Tuple queryPrefix = NonClusterHashIndexRecord.getPrefixQueryTuple(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, targetAttrHashValue);
            List<KeyValue> indexKVPairs = FDBHelper.getPrefixKVPairs(db, employeeTable, queryPrefix);

            // interpret the key-value pairs into index records
            List<NonClusterHashIndexRecord> indexRecords = new ArrayList<>();
            for (KeyValue kv : indexKVPairs) {
                Tuple keyTuple = employeeTable.unpack(kv.getKey());
                indexRecords.add(new NonClusterHashIndexRecord(keyTuple));
            }

            // for each index record,
            for (NonClusterHashIndexRecord record : indexRecords) {
                try {
                    // get the primary key from it
                    Long pkVal = record.getPkValue();
                    // get the attribute name that the index is built on
                    String attrName = record.getHashAttrName();
                    // get the value of the "Name" attribute with certain primary key value
                    String name = (String) FDBHelper.get(db, employeeTable, pkVal, attrName);
                    if (name.equals(targetAttrValue)) {
                        // if find "Bob", query the whole row to show the result.
                        Employee e = queryEmployeeTableWithPrimaryKey(db, employeeTable, record.getPkValue());
                        System.out.println(e);
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
