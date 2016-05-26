package simpledb;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private Map<Field, Item> map;
    private Item item;
    private int gbfield; 
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    public static class Item {
        public int sum;
        public int count;
        public int min;
        public int max;
        
        public Item() {
            sum = count = 0;
            max = Integer.MIN_VALUE;
            min = Integer.MAX_VALUE;
        }
        
        public Item(int val) {
            count = 1;
            min = max = sum = val;
        }
        
        public void mergeValue(int val) {
            sum += val;
            count += 1;
            min = Math.min(min, val);
            max = Math.max(max, val);
        }
        
        public int avg() {
            return sum / count;
        }
    }

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        map = new ConcurrentHashMap<>();
        item = new Item();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField af = (IntField) tup.getField(afield);
        int val = af.getValue();
        if (gbfield != NO_GROUPING) {
            Field gbf = tup.getField(gbfield);
            if (map.containsKey(gbf)) {
                map.get(gbf).mergeValue(val);
            } else {
                map.put(gbf, new Item(val));
            }
        } else {
            item.mergeValue(val);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfield != NO_GROUPING) {
            td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
            for (Entry<Field, Item> e: map.entrySet()) {
                Tuple t = new Tuple(td);
                setTupleField(1, what, t, e.getValue());
                t.setField(0, e.getKey());
                tuples.add(t);
            }
        } else {
            td = new TupleDesc(new Type[] {Type.INT_TYPE});
            Tuple t = new Tuple(td);
            setTupleField(0, what, t, item);
            tuples.add(t);
        }
        
        return new TupleIterator(td, tuples);
    }
    
    private void setTupleField(int fieldNum, Op what, Tuple t, Item item) {
        if (what == Aggregator.Op.SUM)
            t.setField(fieldNum, new IntField(item.sum));
        else if (what == Aggregator.Op.COUNT)
            t.setField(fieldNum, new IntField(item.count));
        else if (what == Aggregator.Op.MAX)
            t.setField(fieldNum, new IntField(item.max));
        else if (what == Aggregator.Op.MIN)
            t.setField(fieldNum, new IntField(item.min));
        else if (what == Aggregator.Op.AVG)
            t.setField(fieldNum, new IntField(item.avg()));
    }

}
