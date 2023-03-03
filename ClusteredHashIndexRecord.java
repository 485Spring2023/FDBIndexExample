import com.apple.foundationdb.tuple.Tuple;

public class ClusteredHashIndexRecord {
  public static String INDEX_TYPE = "HashCluster";

  private String tableName;
  private String indexType;
  private String hashAttrName;
  private Long hashValue;
  private Long pkValue;
  private String attrName;
  private Object attrValue;

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getIndexType() {
    return indexType;
  }

  public void setIndexType(String indexType) {
    this.indexType = indexType;
  }

  public String getHashAttrName() {
    return hashAttrName;
  }

  public void setHashAttrName(String hashAttrName) {
    this.hashAttrName = hashAttrName;
  }

  public Long getHashValue() {
    return hashValue;
  }

  public void setHashValue(Long hashValue) {
    this.hashValue = hashValue;
  }

  public Long getPkValue() {
    return pkValue;
  }

  public void setPkValue(Long pkValue) {
    this.pkValue = pkValue;
  }

  public String getAttrName() {
    return attrName;
  }

  public void setAttrName(String attrName) {
    this.attrName = attrName;
  }

  public Object getAttrValue() {
    return attrValue;
  }

  public void setAttrValue(Object attrValue) {
    this.attrValue = attrValue;
  }

  public ClusteredHashIndexRecord(String tableName, String hashAttrName, Long hashValue, Long pkValue, String attrName, Object attrValue) {
    this.tableName = tableName;
    this.indexType = INDEX_TYPE;
    this.hashAttrName = hashAttrName;
    this.hashValue = hashValue;
    this.pkValue = pkValue;
    this.attrName = attrName;
    this.attrValue = attrValue;
  }


  // key format: (TableName, IndexType, HashAttributeName, HashValue, PrimaryKeyValue, AttributeName)
  public Tuple getKeyTuple() {
    Tuple keyTuple = new Tuple();

    keyTuple = keyTuple.add(tableName).add(indexType).add(hashAttrName).add(hashValue).add(pkValue).add(attrName);
    return keyTuple;
  }

  public static Object getPrimaryKeyFromKeyTuple(Tuple keyTuple) {
    return keyTuple.get(4);
  }

  public static Object getAttributeNameFromKeyTuple(Tuple keyTuple) {
    return keyTuple.get(5);
  }

  public Tuple getValueTuple() {
    Tuple valueTuple = new Tuple();
    valueTuple = valueTuple.addObject(attrValue);
    return valueTuple;
  }

  public static Tuple getPrefixQueryTuple(String tableName, String hashAttrName, Long hashValue) {
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(tableName).add(INDEX_TYPE).add(hashAttrName).add(hashValue);
    return keyTuple;
  }

  public static Tuple getExactMatchQueryTuple(String tableName, String hashAttrName, Long hashValue, Long pkValue, String attrValue) {
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(tableName).add(INDEX_TYPE).add(hashAttrName).add(hashValue).add(pkValue).add(attrValue);
    return keyTuple;
  }
}
