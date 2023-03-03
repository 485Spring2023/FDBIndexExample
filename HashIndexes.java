import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class HashIndexes {

  public static HashMap<String, Long> nameHashMap = new HashMap<String, Long>() {{
    put("Bob", 10L);
    put("Shuqin", 5L);
    put("Alice", 10L);
  }};

  // generate a list of non-cluster hash index records
  public static List<NonClusteredHashIndexRecord> buildNonClusteredHashIndex(Employee[] employees, String targetAttrToBuildIndex) {
    List<NonClusteredHashIndexRecord> records = new ArrayList<>();

    for (int i = 0; i<employees.length; i++) {
      Employee e = employees[i];
      Long hashVal = nameHashMap.get(e.getName());
      NonClusteredHashIndexRecord idxRecord = new NonClusteredHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN());
      records.add(idxRecord);
    }
    return records;
  }

  // generate a list of cluster hash index records
  public static List<ClusteredHashIndexRecord> buildClusteredHashIndex(Employee[] employees, String targetAttrToBuildIndex) {
    List<ClusteredHashIndexRecord> records = new ArrayList<>();

    for (int i = 0; i<employees.length; i++) {
      Employee e = employees[i];
      Long hashVal = nameHashMap.get(e.getName());

      ClusteredHashIndexRecord idxRecordSSNAttr = new ClusteredHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_SSN, e.getSSN());
      ClusteredHashIndexRecord idxRecordNameAttr = new ClusteredHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_NAME, e.getName());
      ClusteredHashIndexRecord idxRecordSalaryAttr = new ClusteredHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_SALARY, e.getSalary());
      ClusteredHashIndexRecord idxRecordDnoAttr = new ClusteredHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_DNO, e.getDno());

      records.add(idxRecordSSNAttr);
      records.add(idxRecordNameAttr);
      records.add(idxRecordSalaryAttr);
      records.add(idxRecordDnoAttr);
    }

    return records;
  }
}
