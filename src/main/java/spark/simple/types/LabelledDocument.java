package spark.simple.types;

public class LabelledDocument extends Document {
	
	private int label;
	
	public LabelledDocument(String id, String text, String source, int label) {
		super(id, text, source);
		this.label = label;
	}

	public int getLabel() {
		return label;
	}

	public void setLabel(int label) {
		this.label = label;
	}
	
	
}
