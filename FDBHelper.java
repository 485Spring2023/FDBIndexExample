import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class FDBHelper {

  public static void clear(Database db) {
    Transaction tx = openTransaction(db);
    final byte[] st = new Subspace(new byte[]{(byte) 0x00}).getKey();
    final byte[] en = new Subspace(new byte[]{(byte) 0xFF}).getKey();
    tx.clear(st, en);
    commitTransaction(tx);
  }

  public static Transaction openTransaction(Database db) {
    return db.createTransaction();
  }

  public static void commitTransaction(Transaction tx) {
    tx.commit().join();
  }

  public static void putKVPair(Transaction tx, DirectorySubspace table, Tuple key, Tuple value) {
    tx.set(table.pack(key), value.pack());
  }

  public static List<KeyValue> getPrefixKVPairs(Database db, DirectorySubspace table, Tuple queryPrefix) {
    Range prefixRange = Range.startsWith(table.pack(queryPrefix));
    ReadTransaction tx = db.createTransaction();
    List<KeyValue> res = tx.getRange(prefixRange).asList().join();
    return res;
  }

  public static Object get(Database db, DirectorySubspace table, Long primaryKey, String attrName) throws ExecutionException, InterruptedException {
    Transaction tx = openTransaction(db);
    Tuple keyTuple = new Tuple();
    keyTuple = keyTuple.add(primaryKey).add(attrName);

    Object value = Tuple.fromBytes(tx.get(table.pack(keyTuple)).get()).get(0);

    commitTransaction(tx);
    return value;
  }
}
