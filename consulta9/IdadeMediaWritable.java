package olympics.consulta9;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IdadeMediaWritable implements Writable {
    private float sum;
    private int count;

    public IdadeMediaWritable() {
        this.sum = 0;
        this.count = 0;
    }

    public IdadeMediaWritable(float sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public float getSum() {
        return sum;
    }

    public int getCount() {
        return count;
    }

    public void setSum(float sum) {
        this.sum = sum;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(sum);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.sum = dataInput.readFloat();
        this.count = dataInput.readInt();
    }
}
