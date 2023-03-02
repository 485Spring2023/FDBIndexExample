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
  public static List<NonClusterHashIndexRecord> buildNonClusterHashIndex(Employee[] employees, String targetAttrToBuildIndex) {
    List<NonClusterHashIndexRecord> records = new ArrayList<>();

    for (int i = 0; i<employees.length; i++) {
      Employee e = employees[i];
      Long hashVal = nameHashMap.get(e.getName());
      NonClusterHashIndexRecord idxRecord = new NonClusterHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN());
      records.add(idxRecord);
    }
    return records;
  }

  // generate a list of cluster hash index records
  public static List<ClusterHashIndexRecord> buildClusterHashIndex(Employee[] employees, String targetAttrToBuildIndex) {
    List<ClusterHashIndexRecord> records = new ArrayList<>();

    for (int i = 0; i<employees.length; i++) {
      Employee e = employees[i];
      Long hashVal = nameHashMap.get(e.getName());

      ClusterHashIndexRecord idxRecordSSNAttr = new ClusterHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_SSN, e.getSSN());
      ClusterHashIndexRecord idxRecordNameAttr = new ClusterHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_NAME, e.getName());
      ClusterHashIndexRecord idxRecordSalaryAttr = new ClusterHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_SALARY, e.getSalary());
      ClusterHashIndexRecord idxRecordDnoAttr = new ClusterHashIndexRecord(Employee.EMPLOYEE_TABLENAME, targetAttrToBuildIndex, hashVal, e.getSSN(), Employee.EMPLOYEE_ATTRIBUTE_DNO, e.getDno());

      records.add(idxRecordSSNAttr);
      records.add(idxRecordNameAttr);
      records.add(idxRecordSalaryAttr);
      records.add(idxRecordDnoAttr);
    }

    return records;
  }
}
