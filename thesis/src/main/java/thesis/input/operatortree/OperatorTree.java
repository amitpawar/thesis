package thesis.input.operatortree;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.FilterOperatorBase;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.translation.JavaPlan;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.dag.DagConnection;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;

public class OperatorTree {

	private JavaPlan javaPlan;
	private Optimizer optimizer;
	private OptimizedPlan optimizedPlan;
	private List<SingleOperator> operators;
	private List<String> addedNodes;

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
			System.out.println(this.operators.get(i).getOperatorName());// .getOperatorType().name());
			System.out.println(this.operators.get(i).getOperatorOutputType().toString());
		}
	}

	public void addOutgoingNodes(List<DagConnection> outgoingConnections) {
		for (DagConnection conn : outgoingConnections) {
			SingleOperator op = new SingleOperator();
			OptimizerNode node = conn.getTarget().getOptimizerNode();
			
			/* op.setOperatorName(node.getName());
			 op.setOperatorOutputType(node.getOperator().getOperatorInfo().getOutputType());
			 this.operators.add(op);*/
			 
			addNode(node);
			if (node.getOptimizerNode().getOutgoingConnections() != null)
				addOutgoingNodes(node.getOptimizerNode()
						.getOutgoingConnections());
		}
	}

	public void addNode(OptimizerNode node) {

		Operator<?> operator = node.getOptimizerNode().getOperator();
		SingleOperator opToAdd = new SingleOperator();

		if (operator instanceof JoinOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.JOIN);
				opToAdd.setOperatorName(operator.getName());
				opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
				this.operators.add(opToAdd);
				this.addedNodes.add(operator.getName());
			}
		}

		if (operator instanceof CrossOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.CROSS);
				opToAdd.setOperatorName(operator.getName());
				opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
				this.operators.add(opToAdd);
				this.addedNodes.add(operator.getName());
			}
		}

		if (operator instanceof FilterOperatorBase) {
			if (!isVisited(operator)) {
				opToAdd.setOperatorType(OperatorType.FILTER);
				opToAdd.setOperatorName(operator.getName());
				opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
				this.operators.add(opToAdd);
				this.addedNodes.add(operator.getName());
			}
		}

		if (operator instanceof GroupReduceOperatorBase) {
			if (operator.getName().contains("Distinct")) {
				if (!isVisited(operator)) {
					opToAdd.setOperatorType(OperatorType.DISTINCT);
					opToAdd.setOperatorName(operator.getName());
					opToAdd.setOperatorOutputType(operator.getOperatorInfo().getOutputType());
					this.operators.add(opToAdd);
					this.addedNodes.add(operator.getName());
				}
			}
		}

	}

}
