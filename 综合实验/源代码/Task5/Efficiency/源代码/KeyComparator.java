import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator{
    protected KeyComparator() {
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        double freqA=Double.parseDouble(a.toString());
        double freqB=Double.parseDouble(b.toString());
        return  -Double.compare(freqA,freqB);
    }
}
