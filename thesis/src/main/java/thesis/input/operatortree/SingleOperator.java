package thesis.input.operatortree;

import org.apache.flink.api.common.typeinfo.TypeInformation;

public class SingleOperator {
	
	private String name;
	private OperatorType operatorType;
	private String previousOperator;
	private OperatorType previousOperatorType;
	private TypeInformation<?> operatorOutputType;
	
	public TypeInformation<?> getOperatorOutputType() {
		return operatorOutputType;
	}

	public void setOperatorOutputType(TypeInformation<?> operatorOutputType) {
		this.operatorOutputType = operatorOutputType;
	}

	public String getOperatorName(){
		return this.name;
	}
	
	public void setOperatorName(String name){
		this.name = name;
	}
	
	public String getPreviousOperatorName(){
		return this.previousOperator;
	}
	
	public void setPreviousOperatorName(String name){
		this.previousOperator = name;
	}
	
	public OperatorType getOperatorType(){
		return this.operatorType;
	}
	
	public void setOperatorType(OperatorType type){
		this.operatorType = type;
	}
	
	public OperatorType getPreviousOperatorType(){
		return this.previousOperatorType;
	}
	
	public void setPreviousOperatorType(OperatorType type){
		this.previousOperatorType = type;
	}
}
