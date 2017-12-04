import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class StringAndInt2 implements Comparable<StringAndInt2> , Writable{

	
	Text tag ;
	private Integer nbrOcc ;
	

	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		   this.tag.readFields(arg0);
	       this.nbrOcc = arg0.readInt();
	}



	@Override
	public void write(DataOutput arg0) throws IOException {
        this.tag.write(arg0);
        arg0.writeInt(this.nbrOcc);
	}


	@Override
	public int compareTo(StringAndInt2 o) {
		if (this.nbrOcc < o.nbrOcc) {
			return 1 ;
					
		}else if (this.nbrOcc > o.nbrOcc){
			return -1 ;
		}else {
			return 0 ;
		}
	}



	public StringAndInt2() {
		this.tag = new Text();
		this.nbrOcc = 0 ;
	}



	public StringAndInt2(Text tag, Integer nbrOcc) {
		super();
		this.tag = tag;
		this.nbrOcc = nbrOcc;
	}


	public Integer getNbrOcc() {
		return nbrOcc;
	}



	public void setNbrOcc(Integer nbrOcc) {
		this.nbrOcc = nbrOcc;
	}



	public Text getTag() {
		return tag;
	}



	public void setTag(Text tag) {
		this.tag = tag;
	}
	
	
}
