package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Random;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@Setter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message,LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
//		String key1;
//		String key2;
		Column column1;
		Column column2;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceivedColumnMessage implements Message,LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -5667745204456518160L;
		int taskId;
		Column column1;
		String key1;
		Column column2;
		String key2;
		boolean areBothColumnsAreMissing;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private HashMap<String,Column> columnHashMap = new HashMap<>();
	private TaskMessage taskMessage;
	private String key1;
	private String key2;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(ReceivedColumnMessage.class,this::handle)
				.build();
	}
	private Behavior<Message> handle(ReceivedColumnMessage message){
		this.getContext().getLog().info("I am a Worker and got a ReceivedColumnMessage");
		if(message.areBothColumnsAreMissing){
			this.getContext().getLog().info("I am a Worker and got a ReceivedColumnMessage which two Columns are missing");
			this.columnHashMap.put(message.getKey1(),message.getColumn1());
			this.columnHashMap.put(message.key2,message.getColumn2());
		}else {
			this.getContext().getLog().info("I am a Worker and got a ReceivedColumnMessage which one Column is missing");
			this.columnHashMap.put(message.getKey1(),message.getColumn1());
		}
		//findInclusionDependency();
		return this;
	}
	private void findInclusionDependency(TaskMessage message){
		Column column1 = message.getColumn1();
		Column column2 = message.getColumn2();
		this.getContext().getLog().info("Checking IND in {} and {} ",column1.getColumnName(),column2.getColumnName());
		Boolean result = column1.getValues().containsAll(column2.getValues());
		if(result){
			this.getContext().getLog().info("found IND between {} and {} ",column1.getColumnName(),column2.getColumnName());
		}else {
			this.getContext().getLog().info("found NO IND between {} and {} ",column1.getColumnName(),column2.getColumnName());
		}
		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), message.getTask(),column1,column2,result);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage,message.getDependencyMinerLargeMessageProxy()));
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(),this.largeMessageProxy));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("got a tastMessage");
		//this.taskMessage = message;
//		key1 = message.key1;
//		key2 = message.key2;
//		this.getContext().getLog().info("getting the Keys which are {} and {}. " , key1,key2);
//		if(! columnHashMap.containsKey(key1) && ! columnHashMap.containsKey(key2)){
//			this.getContext().getLog().info(" I am a worker and need two columns");
//			LargeMessageProxy.LargeMessage requestMessage = new DependencyMiner.getRequiredColumnMessage(this.largeMessageProxy,
//					this.getContext().getSelf(), message.getTask(), message.getKey1(), message.getKey2(), true
//			);
//			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestMessage,message.getDependencyMinerLargeMessageProxy()));
//		} else if (!columnHashMap.containsKey(key1)) {
//			this.getContext().getLog().info(" I am a worker and need first column");
//			LargeMessageProxy.LargeMessage requestMessage = new DependencyMiner.getRequiredColumnMessage(this.largeMessageProxy,
//					this.getContext().getSelf(), message.getTask(), message.getKey1(), null, false
//			);
//			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestMessage,message.getDependencyMinerLargeMessageProxy()));
//
//		} else if (!columnHashMap.containsKey(key2)) {
//			this.getContext().getLog().info(" I am a worker and need second column");
//			LargeMessageProxy.LargeMessage requestMessage = new DependencyMiner.getRequiredColumnMessage(this.largeMessageProxy,
//					this.getContext().getSelf(), message.getTask(), message.getKey2(), null, false
//			);
//			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(requestMessage,message.getDependencyMinerLargeMessageProxy()));
//		}else {
			findInclusionDependency(message);
//		}

//		this.getContext().getLog().info("Working!");
//		// I should probably know how to solve this task, but for now I just pretend some work...
//
//		int result = message.getTask();
//		long time = System.currentTimeMillis();
//		Random rand = new Random();
//		int runtime = (rand.nextInt(2) + 2) * 1000;
//		while (System.currentTimeMillis() - time < runtime)
//			result = ((int) Math.abs(Math.sqrt(result)) * result) % 1334525;
//
//		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), result);
//		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));

		return this;
	}
}
