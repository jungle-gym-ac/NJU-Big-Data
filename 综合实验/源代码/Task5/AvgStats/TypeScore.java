import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TypeScore implements WritableComparable<TypeScore>{
    private String type;
    private String name;
    private double score;

    public String getType() {
        return this.type;
    }

    public String getName() {
        return this.name;
    }

    public double getScore() {
        return this.score;
    }

    public TypeScore() {}

    public TypeScore(String type, double score, String team) {
        this.type = type;
        this.score = score;
        this.name = team;
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
        dataOutput.writeDouble(score);
        dataOutput.writeUTF(name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.type = dataInput.readUTF();
        this.score = dataInput.readDouble();
        this.name = dataInput.readUTF();
    }
}
