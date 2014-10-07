import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeWritable implements Writable{
    private  String fileName;
    private int count;

    public CompositeWritable(String fileName, int count){
        this.fileName=fileName;
        this.count=count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        WritableUtils.writeString(dataOutput,fileName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.count=dataInput.readInt();
        this.fileName = WritableUtils.readString(dataInput);
    }

    @Override
    public String toString() {
        return fileName+"\t"+count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompositeWritable that = (CompositeWritable) o;

        if (count != that.count) return false;
        if (fileName != null ? !fileName.equals(that.fileName) : that.fileName != null) return false;

        return true;
    }


    @Override
    public int hashCode() {
        int result = fileName != null ? fileName.hashCode() : 0;
        result = 31 * result + count;
        return result;
    }
}
