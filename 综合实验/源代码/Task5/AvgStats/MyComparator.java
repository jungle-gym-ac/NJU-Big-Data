import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
public class MyComparator extends WritableComparator {//从大到小排序
    public MyComparator(){
        super(TypeScore.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TypeScore a1 = (TypeScore) a;
        TypeScore b1 = (TypeScore) b;
        if (b1.getScore() > a1.getScore())
            return 1;
        else
            return -1;
    }
}