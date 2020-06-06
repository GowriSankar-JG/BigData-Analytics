import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author qiang
 */
public class ProductPair implements WritableComparable<ProductPair>{
    private String product1;
    private String product2;
    
    @Override
    public void write(DataOutput d) throws IOException {
        d.writeUTF(product1);
        d.writeUTF(product2);
    }

    @Override
    public void readFields(DataInput di) throws IOException {
        this.product1 = di.readUTF();
        this.product2 = di.readUTF();
    }

    @Override
    public int compareTo(ProductPair o) {
        int v = this.product1.compareTo(o.product1);
        return v == 0 ? this.product2.compareTo(o.product2) : v;
    }

    public String getProduct1() {
        return product1;
    }

    public void setProduct1(String product1) {
        this.product1 = product1;
    }

    public String getProduct2() {
        return product2;
    }

    public void setProduct2(String product2) {
        this.product2 = product2;
    }
}
