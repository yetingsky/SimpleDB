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
    
    public static class Item {
        public int count;
        
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
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbf = tup.getField(gbfield);
        StringField af = (StringField) tup.getField(afield);
        if (map.containsKey(gbf)) {
            map.get(gbf).mergeValue(af.getValue());
        } else {
            map.put(gbf, new Item(af.getValue()));
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
        TupleDesc td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        ArrayList<Tuple> tuples = new ArrayList<>();
        for (Entry<Field, Item> e: map.entrySet()) {
            Tuple t = new Tuple(td);
            t.setField(0, e.getKey());
            if (what == Aggregator.Op.COUNT)
                t.setField(1, new IntField(e.getValue().count));
            tuples.add(t);
        }
        return new TupleIterator(td, tuples);
    }

}
