import com.apple.foundationdb.tuple.Tuple;

public class NonClusterHashIndexRecord {

  public static String INDEX_TYPE = "HashNonCluster";

  private String tableName;
  private String indexType;
  private String hashAttrName;
  private Long hashValue;
  private Long pkValue;


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

  public NonClusterHashIndexRecord(String tableName, String hashAttrName, Long hashValue, Long pkValue) {
    this.tableName = tableName;
    this.indexType = INDEX_TYPE;
    this.hashAttrName = hashAttrName;
    this.hashValue = hashValue;
    this.pkValue = pkValue;
  }

  public NonClusterHashIndexRecord(Tuple keyTuple) {
    this.tableName = (String) keyTuple.get(0);
    this.indexType = (String) keyTuple.get(1);
    this.hashAttrName = (String) keyTuple.get(2);
    this.hashValue = (Long) keyTuple.get(3);
    this.pkValue = (Long) keyTuple.get(4);
  }

  public Tuple getKeyTuple() {
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(tableName).add(indexType).add(hashAttrName).add(hashValue).add(pkValue);
    return keyTuple;
  }

  public Tuple getValueTuple() {
    Tuple valueTuple = new Tuple();
    return valueTuple;
  }

  public static Tuple getPrefixQueryTuple(String tableName, String hashAttrName, Long hashValue) {
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(tableName).add(INDEX_TYPE).add(hashAttrName).add(hashValue);
    return keyTuple;
  }
}
