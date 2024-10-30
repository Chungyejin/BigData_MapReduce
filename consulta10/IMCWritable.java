package olympics.consulta10;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IMCWritable implements Writable {
    private float altura;
    private float peso;

    public IMCWritable(float altura, float peso) {
        this.altura = altura;
        this.peso = peso;
    }

    public IMCWritable() {
    }

    public float getAltura() {
        return altura;
    }

    public void setAltura(float altura) {
        this.altura = altura;
    }

    public float getPeso() {
        return peso;
    }

    public void setPeso(float peso) {
        this.peso = peso;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(altura);
        dataOutput.writeFloat(peso);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.altura = dataInput.readFloat();
        this.peso = dataInput.readFloat();
    }
}
