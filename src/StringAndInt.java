
public class StringAndInt implements Comparable<StringAndInt> {
	
	String tag ;
	private Integer nbrOcc ;
	
	
	@Override
	public int compareTo(StringAndInt o) {
		return Integer.compare(o.getNbrOcc(), this.nbrOcc);
	}



	public StringAndInt(String tag, Integer nbrOcc) {
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



	public String getTag() {
		return tag;
	}



	public void setTag(String tag) {
		this.tag = tag;
	}
	
	
}
