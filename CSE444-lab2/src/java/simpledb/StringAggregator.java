package simpledb;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private Map<Field, Item> map;
    private int gbfield; 
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Item item;  // for non-grouping
    
    public static class Item {
        public int count;
        
        public Item() {
            count = 0;
        }
        
        public Item(String val) {
            count = 1;
        }
        
        public void mergeValue(String val) {
            count += 1;
        }
    }

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield  =afield;
        this.what = what;
        map = new ConcurrentHashMap<>();
        item = new Item();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField af = (StringField) tup.getField(afield);
        String val = af.getValue();
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
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
        if (what == Aggregator.Op.COUNT)
            t.setField(fieldNum, new IntField(item.count));
    }

}
