package olympics.consulta8;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MaxNameWritable implements Writable {
    String athlete;
    float max_height;

    public MaxNameWritable() {
    }

    public MaxNameWritable(String athlete, float max_height) {
        this.athlete = athlete;
        this.max_height = max_height;
    }

    public String getAthlete() {
        return athlete;
    }

    public void setAthlete(String athlete) {
        this.athlete = athlete;
    }

    public float getMax_height() {
        return max_height;
    }

    public void setMax_height(float max_height) {
        this.max_height = max_height;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.athlete);
        dataOutput.writeFloat(this.max_height);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.athlete = dataInput.readUTF();
        this.max_height = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "{max height = " + max_height +
                ", athlete = " + athlete +
                '}';
    }
}
