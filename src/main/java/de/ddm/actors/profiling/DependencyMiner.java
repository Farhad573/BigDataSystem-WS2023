package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		// Here we add the large Message Proxy of the worker so that we can find it easily.
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		// this is the Message which Dependency worker sends to Miner when the worker finished comparing two columns
		// and asks for another task if available
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		int taskID;
		Column column1;
		Column column2;
		boolean foundIND;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
		// here we store LargeMessageProxy of all workers which the Minor creates
		this.dependencyWorkersLargeMessageProxy = new ArrayList<>();
		this.dependencyWorkers = new ArrayList<>();
		// we use this counter to see how many of the CSV files are already read and how many are not

		this.fileCounter = inputFiles.length;

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;
	// After reading all Batches we save all the Columns here in this Hashmap. The key is the Name of the Column and Value is the Column Object which contains the data.
	private HashMap<String,Column> columnHashMap = new HashMap<>();
	// Here we save all Tasks to know how many tasks are there in total

	private List<DependencyWorker.TaskMessage> taskMessageList = new ArrayList<>();
	private  List<ActorRef<LargeMessageProxy.Message>> dependencyWorkersLargeMessageProxy;
	// this counter is there to see how many tasks are still there to be done

	private int taskCounter = 0;
	private int fileCounter;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {

		this.getContext().getLog().info("Received batch of {} rows for file {}!", message.getBatch().size(), this.inputFiles[message.getId()].getName());
		// Here are all rows in a Batch
		List<String[]> rows = message.getBatch();

		if(!rows.isEmpty()){
			// number of a columns in a row
			int numberOfColumns = rows.get(0).length;
			for (int columnNumber = 0; columnNumber < numberOfColumns; columnNumber++){
				for (String[] row : rows){
					// for each row we put the data of that column the Hashmap
					putInHashMapOfColumns(message,columnNumber,row);
				}
			}
			// And then we ask for another Batch
			this.inputReaders.get(message.getId()).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}else {
			// when we get a empty Batch, it means reading a file is finished
			this.getContext().getLog().info("Reading file {} is finished", this.inputFiles[message.getId()].getName());
			fileCounter--;
			// then we check here if there is file to be read or not, if not we start making tasks and check the columns
			if (fileCounter == 0) {
				this.getContext().getLog().info("All files have been read");
				startChecking();
			}
		}
		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// we save the worker LargeMessage proxy so that we can find it later
			this.dependencyWorkersLargeMessageProxy.add(message.getDependencyWorkerLargeMessageProxy());
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		// The completion Message has a boolean which says if the Worker found a IND or not. If it is True we send the two Columns to ResultCollector
		if (message.isFoundIND()) {
			File dependentFile = new File(message.getColumn2().getNameOfFile());
			File referencedFile = new File(message.getColumn1().getNameOfFile());
			String[] dependentAttributes = new String[]{message.getColumn2().getColumnName()};
			String[] referencedAttributes = new String[]{message.getColumn1().getColumnName()};
			InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
			List<InclusionDependency> inds = new ArrayList<>(1);
			inds.add(ind);

			this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
		}
		// Here when Miner gets a Completion Message, it means that it is now idle and therefore we can send it a new Task
		sendTasksToDependencyWorker(dependencyWorker);
		return this;
	}
	/**
	 * After reading all files are done, this method will be called which starts taking every two column and making a task to send it to the worker
	 */
	private void startChecking(){
		this.getContext().getLog().info("Lets start checking");
		// We make every Two column into a TaskMessage and save it in List of Tasks
		for (String key1 : columnHashMap.keySet()){
			for (String key2 : columnHashMap.keySet()){
				if(! key1.equals(key2)){
					DependencyWorker.TaskMessage task = new DependencyWorker.TaskMessage(this.largeMessageProxy,-1,columnHashMap.get(key1),columnHashMap.get(key2));
					taskMessageList.add(task);
				}
			}
		}
		// And here we send the Tasks to the Workers
		for (ActorRef<DependencyWorker.Message> dependencyWorker : this.dependencyWorkers) {
			sendTasksToDependencyWorker(dependencyWorker);
		}
	}
	/**
	 * @param dependencyWorker
	 * Send a task to the given dependency worker
	 */
	private void sendTasksToDependencyWorker(ActorRef<DependencyWorker.Message> dependencyWorker){
		this.getContext().getLog().info("number of remaining Tasks is {}: ." , taskMessageList.size() - taskCounter);
		this.getContext().getLog().info("Sending task to a worker");
		// if there is still task to be done
		if(checkRemainingTasks()){
			this.getContext().getLog().info("Still tasks remaining to be done");
			DependencyWorker.TaskMessage taskMessage = this.taskMessageList.get(taskCounter);
			taskMessage.setDependencyMinerLargeMessageProxy(this.largeMessageProxy);
			// Here we send the task via Large Message proxy to the Worker
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskMessage,this.dependencyWorkersLargeMessageProxy.get(this.dependencyWorkers.indexOf(dependencyWorker))));
			taskCounter++;
		}else {
			this.getContext().getLog().info("All tasks are given to Workers");
			this.end();
		}
	}
	/**
	 * @return True if there is still task to be done
	 */
	private boolean checkRemainingTasks(){
		if(taskCounter < taskMessageList.size()){
			return true;
		}else
			return false;
	}
	/**
	 * @param message
	 * @param columnNumber
	 * @param row
	 * It puts the data into our HashMap
	 */
	private void putInHashMapOfColumns(BatchMessage message, int columnNumber, String[] row){
		// The Hashmap is Map of String which is the ColumnName and its Value is Data of that Column which is a Column Object.
		// So here we check if the Map has already has the columnName and just add the data to the column
		if(columnHashMap.containsKey(this.headerLines[message.getId()][columnNumber])){
			columnHashMap.get(this.headerLines[message.getId()][columnNumber]).addValueToColumn(row[columnNumber]);
		}else {
			// otherwise we get the ColumnName from headerlines and then add the data to the column
			columnHashMap.put(this.headerLines[message.getId()][columnNumber],new Column(columnNumber,this.headerLines[message.getId()][columnNumber],this.inputFiles[message.getId()].getName()));
			columnHashMap.get(this.headerLines[message.getId()][columnNumber]).addValueToColumn(row[columnNumber]);
		}
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}