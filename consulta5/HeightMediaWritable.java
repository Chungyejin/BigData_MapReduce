package olympics.consulta5;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class HeightMediaWritable implements Writable {
    float totalHeight;
    int count;


    public HeightMediaWritable() {
        this.totalHeight = 0;
        this.count = 0;
    }

    public float getTotalHeight() {
        return totalHeight;
    }

    public void setTotalHeight(float totalHeight) {
        this.totalHeight = totalHeight;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public HeightMediaWritable(float totalHeight, int count) {
        this.totalHeight = totalHeight;
        this.count = count;
    }

    public void add(float height) {
        this.totalHeight += height;
        this.count++;
    }

    public float getAverage() {
        return count == 0 ? 0 : totalHeight / count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(totalHeight);
        out.writeInt(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalHeight = in.readFloat();
        count = in.readInt();
    }
}
