import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Type;

public class TypeScore implements WritableComparable<TypeScore>{
    private String type;
    private int score;

    public String getType() {
        return this.type;
    }

    public int getScore() {
        return this.score;
    }

    public TypeScore() {}

    public TypeScore(String type, int score) {
        this.type = type;
        this.score = score;
    }

    @Override
    public int compareTo(TypeScore o) {
        if (this.score > o.score)
            return 1;
        else
            return -1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(type);
        dataOutput.writeInt(score);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.score = dataInput.readInt();
    }
}
