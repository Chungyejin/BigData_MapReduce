package olympics.consulta10;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IMCAvgWritable implements Writable {
    private float sumIMC;
    private int count;

    public IMCAvgWritable() {
    }

    public IMCAvgWritable(float sumIMC, int count) {
        this.sumIMC = sumIMC;
        this.count = count;
    }

    public float getSumIMC() {
        return sumIMC;
    }

    public void setSumIMC(float sumIMC) {
        this.sumIMC = sumIMC;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(sumIMC);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sumIMC = dataInput.readFloat();
        this.count = dataInput.readInt();
    }
}
