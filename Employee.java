import java.util.ArrayList;
import java.util.List;

public class Employee {
    static String EMPLOYEE_TABLENAME = "Employee";

    // Employee table has 4 attributes: SSN, Name, Salary and Dno. SSN is the
    // primary key
    static String EMPLOYEE_ATTRIBUTE_SSN = "SSN";
    static String EMPLOYEE_ATTRIBUTE_NAME = "Name";
    static String EMPLOYEE_ATTRIBUTE_SALARY = "Salary";
    static String EMPLOYEE_ATTRIBUTE_DNO = "Dno";

    private Long SSN;
    private String Name;
    private Long Salary;
    private Long Dno;

    public static String[] ATTRIBUTES() {
        return new String[] {
            EMPLOYEE_ATTRIBUTE_SSN,
            EMPLOYEE_ATTRIBUTE_NAME,
            EMPLOYEE_ATTRIBUTE_SALARY,
            EMPLOYEE_ATTRIBUTE_DNO
        };
    }

    public Employee() {};

    public Employee(Long SSN, String name, Long salary, Long dno) {
        this.SSN = SSN;
        Name = name;
        Salary = salary;
        Dno = dno;
    }

    @Override
    public String toString() {
        return "Employee(" + "SSN:" + SSN + ", Name:" + Name + ", Salary:" + Salary + ", Dno:" + Dno + ")";
    }

    public Long getSSN() {
        return SSN;
    }

    public void setSSN(Long SSN) {
        this.SSN = SSN;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public Long getSalary() {
        return Salary;
    }

    public void setSalary(Long salary) {
        Salary = salary;
    }

    public Long getDno() {
        return Dno;
    }

    public void setDno(Long dno) {
        Dno = dno;
    }

    public void setAttributeValue(String attrName, Object attrVal) {
        if (attrName.equals(EMPLOYEE_ATTRIBUTE_NAME)) {
            setName((String) attrVal);
        } else if (attrName.equals(EMPLOYEE_ATTRIBUTE_DNO)) {
            setDno((Long) attrVal);
        } else if (attrName.equals(EMPLOYEE_ATTRIBUTE_SALARY)) {
            setSalary((Long) attrVal);
        } else if (attrName.equals(EMPLOYEE_ATTRIBUTE_SSN)) {
            setSSN((Long) attrVal);
        }
    }
}
