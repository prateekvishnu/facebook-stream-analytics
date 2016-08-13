package spark.simple.types;

public class Document {

	private String id;
	private String text;
	private String source;
	
	public Document(String id, String text, String source) {
		this.id = id;
		this.text = text;
		this.source = source;
	}
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public String getSource() {
		return source;
	}
	public void setSource(String source) {
		this.source = source;
	}
	
}
