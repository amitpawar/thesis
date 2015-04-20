package thesis.input.operatortree;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.JoinOperator.JoinOperatorSets.JoinOperatorSetsPredicate;
import org.apache.flink.api.java.operators.UnionOperator;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.api.java.operators.translation.PlanProjectOperator;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.Union;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import thesis.input.operatortree.SingleOperator.JoinCondition;

public class OperatorTree {

	private JavaPlan javaPlan;
	private Optimizer optimizer;
	private OptimizedPlan optimizedPlan;
	private List<SingleOperator> operators;
	private List<String> addedNodes;
	
	private enum InputNum { 
		FIRST(0),SECOND(1);
		private int value;
		private InputNum(int val){
			this.value = val;
		}
		private int getValue(){
			return this.value;
		}
	};

	public OperatorTree(ExecutionEnvironment env) {
		this.javaPlan = env.createProgramPlan();
		this.optimizer = new Optimizer(new DataStatistics(),
				new DefaultCostEstimator());
		this.optimizedPlan = this.optimizer.compile(this.javaPlan);
		this.operators = new ArrayList<SingleOperator>();
		this.addedNodes = new ArrayList<String>();
	}

	public void createOperatorTreeTest() {

		int loadCtr = 1;
		for (SourcePlanNode sourceNode : this.optimizedPlan.getDataSources()) {

			if (!isVisited(sourceNode.getProgramOperator())) {
				SingleOperator op = new SingleOperator();
				op.setOperatorType(OperatorType.LOAD);
				op.setOperatorName(loadCtr++ + ".1 " + sourceNode.getNodeName());
				this.operators.add(op);
				this.addedNodes.add(sourceNode.getNodeName());
			}
		}

		for (PlanNode node : this.optimizedPlan.getAllNodes()) {
			SingleOperator opToAdd = new SingleOperator();
			Operator<?> operator = node.getOptimizerNode().getOperator();

			if (operator instanceof JoinOperatorBase) {
				if (!isVisited(operator)) {
					opToAdd.setOperatorType(OperatorType.JOIN);
					opToAdd.setOperatorName(operator.getName());
					this.operators.add(opToAdd);
					this.addedNodes.add(operator.getName());
				}
			}

			if (operator instanceof CrossOperatorBase) {
				if (!isVisited(operator)) {
					opToAdd.setOperatorType(OperatorType.CROSS);
					opToAdd.setOperatorName(operator.getName());
					this.operators.add(opToAdd);
					this.addedNodes.add(operator.getName());
				}
			}

			if (operator instanceof FilterOperatorBase) {
				if (!isVisited(operator)) {
					opToAdd.setOperatorType(OperatorType.FILTER);
					opToAdd.setOperatorName(operator.getName());
					this.operators.add(opToAdd);
					this.addedNodes.add(operator.getName());
				}
			}

			if (operator instanceof GroupReduceOperatorBase) {
				if (operator.getName().contains("Distinct")) {
					if (!isVisited(operator)) {
						opToAdd.setOperatorType(OperatorType.DISTINCT);
						opToAdd.setOperatorName(operator.getName());
						this.operators.add(opToAdd);
						this.addedNodes.add(operator.getName());
					}
				}
			}
		}

		for (int i = 0; i < this.operators.size(); i++) {
			System.out.println(this.operators.get(i).getOperatorName());// .getOperatorType().name());
		}
	}

	private boolean isVisited(Operator<?> operator) {
		return (this.addedNodes.contains(operator.getName()));
	}

	public void createOperatorTree() {
		for (SourcePlanNode sourceNode : this.optimizedPlan.getDataSources()) {
			if (!isVisited(sourceNode.getProgramOperator())) {
				SingleOperator op = new SingleOperator();
				op.setOperatorType(OperatorType.LOAD);
				op.setOperatorName(sourceNode.getNodeName());
				op.setOperatorOutputType(sourceNode.getOptimizerNode().getOperator().getOperatorInfo().getOutputType());
				this.operators.add(op);
				this.addedNodes.add(sourceNode.getNodeName());
				if (sourceNode.getOptimizerNode().getOutgoingConnections() != null)
					addOutgoingNodes(sourceNode.getOptimizerNode()
							.getOutgoingConnections());
			}
		}
		
		for (int i = 0; i < this.operators.size(); i++) {
			if(!(this.operators.get(i).getOperatorInputType() == null)){
				System.out.println("INPUT :");
				for(TypeInformation<?> inputType : this.operators.get(i).getOperatorInputType())
					System.out.println(inputType+" ");
			}
			System.out.println("NODE :");
			System.out.println(this.operators.get(i).getOperatorName());// .getOperatorType().name());
			System.out.println(this.operators.get(i).getOperatorType().name());
			System.out.println("OUTPUT ");
			System.out.println(this.operators.get(i).getOperatorOutputType().toString());
			if(this.operators.get(i).getJoinCondition() != null)
				System.out.println(this.operators.get(i).getJoinCondition().getFirstInput()+"join("+
						this.operators.get(i).getJoinCondition().getSecontInput()+").where("
						+this.operators.get(i).getJoinCondition().getFirstInputKeyColumns()[0]+").equalsTo("+
						this.operators.get(i).getJoinCondition().getSecondInputKeyColumns()[0]+")");
		}
	}

	public void addOutgoingNodes(List<DagConnection> outgoingConnections) {
		for (DagConnection conn : outgoingConnections) {
			SingleOperator op = new SingleOperator();
			OptimizerNode node = conn.getTarget().getOptimizerNode();
			
		/*	op.setOperatorName(node.getOperator().toString());
			op.setOperatorInputType(addInputTypes(node.getOperator()));
			op.setOperatorOutputType(node.getOperator().getOperatorInfo().getOutputType());
			this.operators.add(op);*/
			 
			addNode(node);
			if (node.getOptimizerNode().getOutgoingConnections() != null)
				addOutgoingNodes(node.getOptimizerNode()
						.getOutgoingConnections());
		}
	}

	@SuppressWarnings("rawtypes")
	public void addNode(OptimizerNode node) {

		Operator<?> operator = node.getOptimizerNode().getOperator();
		SingleOperator opToAdd = new SingleOperator();
		
		if (operator instanceof JoinOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.JOIN);
				SingleOperator opToAddWithJoinPred = addJoinOperatorDetails((JoinOperatorBase) operator, opToAdd);
				addOperatorDetails(opToAddWithJoinPred, operator);
				
			}
		}

		if (operator instanceof CrossOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.CROSS);
				addOperatorDetails(opToAdd, operator);
			}
		}

		if (operator instanceof FilterOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.FILTER);
				addOperatorDetails(opToAdd, operator);
			}
		}
		
		if (operator instanceof Union<?>) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.UNION);
				addOperatorDetails(opToAdd, operator);
			}
		}
		
		if (operator instanceof PlanProjectOperator) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.PROJECT);
				addOperatorDetails(opToAdd, operator);
			}
		}

		if (operator instanceof GroupReduceOperatorBase) {
			if (operator.getName().contains("Distinct")) {
				if (!isVisited(operator)) {
					opToAdd.setOperatorType(OperatorType.DISTINCT);
					addOperatorDetails(opToAdd, operator);
				}
			}
		}
		
		

	}
	
	public void addOperatorDetails(SingleOperator opToAdd, Operator<?> operator){
		opToAdd.setOperatorName(operator.getName());
		opToAdd.setOperatorInputType(addInputTypes(operator));
		opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
		this.operators.add(opToAdd);
		this.addedNodes.add(operator.getName());
	}
	
	@SuppressWarnings("rawtypes")
	public List<TypeInformation<?>> addInputTypes(Operator<?> operator){
		
		List<TypeInformation<?>> inputTypes = new ArrayList<TypeInformation<?>>();
		
		if(operator instanceof DualInputOperator){
			inputTypes.add(((DualInputOperator)operator).getOperatorInfo().getFirstInputType());
			inputTypes.add(((DualInputOperator)operator).getOperatorInfo().getSecondInputType());
		}
		
		if(operator instanceof SingleInputOperator){
			inputTypes.add(((SingleInputOperator)operator).getOperatorInfo().getInputType());
		}
		
		return inputTypes;
	}
	
	public SingleOperator addJoinOperatorDetails(JoinOperatorBase joinOperator, SingleOperator opToAdd){
		
		
		int[] firstInputKeys = joinOperator.getKeyColumns(InputNum.FIRST.getValue());
		int[] secondInputKeys = joinOperator.getKeyColumns(InputNum.SECOND.getValue());
		
		JoinCondition joinPred = opToAdd.new JoinCondition();
		
		joinPred.setFirstInput(InputNum.FIRST.getValue());
		joinPred.setSecontInput(InputNum.SECOND.getValue());
		joinPred.setFirstInputKeyColumns(firstInputKeys);
		joinPred.setSecondInputKeyColumns(secondInputKeys);
		
		
		opToAdd.setJoinCondition(joinPred);
		
		return opToAdd;
	
	}
	
}
