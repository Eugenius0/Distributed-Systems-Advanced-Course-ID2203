/// ### From Best Effort to Causal Order Reliable Broadcast

/// This is the first programming assignment where you will have to build and reuse multiple components.  
/// Starting bottom-up, you will first have to implement _best effort broadcast_, then _reliable broadcast_ and finally _causal order reliable broadcast_.  
/// Mind that passing each component check will give you a **partial** grade and therefore you will need to pass **all** checks to get the full grade for this programming assignment.

/// **Things to Remember**:  
/// 1. Some components such as `PerfectLink`, `Network` and `Timer` are already provided. No need to implement them.  
/// 2. Execute the imports defined below **before** compiling your component implementations.  
/// 3. We recommend making use of the component state and internal messages we have provided, if any, to complete the implementation logic.  
/// 4. You can always print messages to the output log, from within handlers to see what happens during the simulation. e.g. `println(s"Process $self delivers message $msg");`  
/// 5. Remember that during the simulation check you can print and observe the simulation time, i.e. with `System.currentTimeMillis()`.  
/// 5. Do not forget to run the checker code block after each component implementation to ensure that all properties are satisfied before exporting and submitting the notebook.  

/// Good luck! :)

package se.kth.edx.id2203.templates

import se.kth.edx.id2203.core.ExercisePrimitives._
import se.kth.edx.id2203.core.Ports._
import se.kth.edx.id2203.templates.WaitingCRB._
import se.kth.edx.id2203.validation._
import se.sics.kompics.network._
import se.sics.kompics.sl.{ Init, _ }
import se.sics.kompics.{ ComponentDefinition => _, Port => _, _ }

import scala.collection.immutable.Set
import scala.collection.mutable
import scala.collection.mutable._

/// ### Part I: Best-Effort Broadcast

/// A Best-Effort Broadcast Abstraction (BEB), in Kompics terms, is a component that **provides** the following port _(already imported in the notebook)_.

/// ```
/// class BestEffortBroadcast extends Port {
/// indication[BEB_Deliver];
/// request[BEB_Broadcast];
/// }
/// ```
/// 
/// A **BEB** component should request `BEB_Broadcast` and indicate `BEB_Deliver` events as defined below:
///
/// ```
/// case class BEB_Deliver(source: Address, payload: KompicsEvent) extends KompicsEvent;
/// case class BEB_Broadcast(payload: KompicsEvent) extends KompicsEvent;
/// ```
///
/// As you have already learnt from the course lectures, Best-Effort Broadcast should satisfy the following properties:
///
/// 1.  **Validity**: _If a correct process broadcasts a message m, then every correct process eventually delivers m._
/// 2.  **No duplication**: _No message is delivered more than once._
/// 3.  **No creation**: _If a process delivers a message m with sender s, then m was previously broadcast by process s._
/// 
/// HINT: The recommended algorithm to use in this assignment is _Basic Broadcast_ and is described in the following [document](https://d37djvu3ytnwxt.cloudfront.net/assets/courseware/v1/a91e7d1ac75367325c1efd101a9e2138/asset-v1:KTHx+ID2203.1x+2016T3+type@asset+block/basicbroadcast.pdf) in the respective lecture.
class BasicBroadcast(init: Init[BasicBroadcast]) extends ComponentDefinition {

  //BasicBroadcast Subscriptions
  val pLink = requires[PerfectLink];
  val beb = provides[BestEffortBroadcast];

  //BasicBroadcast Component State and Initialization
  val (self, topology) = init match {
    case Init(s: Address, t: Set[Address] @unchecked) => (s, t)
  };

  //BasicBroadcast Event Handlers
  beb uponEvent {
    case x: BEB_Broadcast => {
        /* WRITE YOUR CODE HERE */
        for(p<-topology){
          trigger(PL_Send(p,x)->pLink);
        }
    }
  }

  pLink uponEvent {
    case PL_Deliver(src, BEB_Broadcast(payload)) => {
        /* WRITE YOUR CODE HERE */
        trigger(BEB_Deliver(src,payload) -> beb);
    }
  }
}

/// ### Part II: Reliable Broadcast

/// A Reliable Broadcast Abstraction (RB), in Kompics terms, is a component that **provides** the following port _(already imported in the notebook)_.

/// ```
/// class ReliableBroadcast extends Port {
/// indication[RB_Deliver];
/// request[RB_Broadcast];
/// }
/// ```
/// 
/// An **RB** component should request `RB_Broadcast` and indicate `RB_Deliver` events, as defined below:
/// 
/// ```
/// case class RB_Deliver(source: Address, payload: KompicsEvent) extends KompicsEvent;
/// case class RB_Broadcast(payload: KompicsEvent) extends KompicsEvent;
/// ```
/// 
/// As you have already learnt from the course lectures, Reliable Broadcast adds the `Agreement` property into the already existing properties of Best-Effort Broadcast:
/// 
/// 1.  Validity: _If a correct process broadcasts a message m, then every correct process eventually delivers m._
/// 2.  No duplication: _No message is delivered more than once._
/// 3.  No creation: _If a process delivers a message m with sender s, then m was previously broadcast by process s._
/// 4.  **Agreement**: _If a message m is delivered by some correct process, then m is eventually delivered by every correct process._
/// 
/// HINT: The recommended algorithm to use in this assignment is _Eager Reliable Broadcast_ and is described in page 2 within the following [document](https://d37djvu3ytnwxt.cloudfront.net/assets/courseware/v1/6c144fd806b3568f6e2c5d7d03e27a29/asset-v1:KTHx+ID2203.1x+2016T3+type@asset+block/reliablebroadcast.pdf) in the respective lecture.
/// 
/// **Mind that, to complete this part, you will first have to implement and test Best-Effort Broadcast, defined above.**
class EagerReliableBroadcast(init: Init[EagerReliableBroadcast]) extends ComponentDefinition {
  //EagerReliableBroadcast Subscriptions
  val beb = requires[BestEffortBroadcast];
  val rb = provides[ReliableBroadcast];

  //EagerReliableBroadcast Component State and Initialization
  val self = init match {
    case Init(s: Address) => s
  };
  val delivered = mutable.Set[KompicsEvent]();

  //EagerReliableBroadcast Event Handlers
  rb uponEvent {
    case x @ RB_Broadcast(payload) => {
        /* WRITE YOUR CODE HERE */
        trigger(BEB_Broadcast(RB_Broadcast(payload)) -> beb);
    }
  }

  beb uponEvent {
    case BEB_Deliver(src, y @ RB_Broadcast(payload)) => {
        /* WRITE YOUR CODE HERE */
        delivered += payload;
        trigger(RB_Deliver(src, payload) -> rb);
        trigger(BEB_Broadcast(RB_Broadcast(payload)) -> beb);
    }
  }
}

//Causal Reliable Broadcast
//Declare custom message types related to internal component implementation
object WaitingCRB {

  case class DataMessage(timestamp: VectorClock, payload: KompicsEvent) extends KompicsEvent;
}

/// ### Part III: Causal-Order Reliable Broadcast
/// 
/// A Causal-Order Reliable Broadcast Abstraction (CRB), in Kompics terms, is a component that **provides** the following port _(already imported in the notebook)_.
/// 
/// ```
/// class CausalOrderReliableBroadcast extends Port {
///  indication[CRB_Deliver];
///  request[CRB_Broadcast];
/// }
/// ```
/// 
/// A **CRB** component should request `CRB_Broadcast` and indicate `CRB_Deliver` events, as defined below:
/// 
/// ```
/// case class CRB_Deliver(src: Address, payload: KompicsEvent) extends KompicsEvent;
/// case class CRB_Broadcast(payload: KompicsEvent) extends KompicsEvent;
/// ```
/// 
/// As you have already learnt from the course lectures, Causal-Order Reliable Broadcast adds the `Causal Delivery` property into the already existing properties of Reliable and Best-Effort Broadcast:
/// 
/// 1.  Validity: _If a correct process broadcasts a message m, then every correct process eventually delivers m._
/// 2.  No duplication: _No message is delivered more than once._
/// 3.  No creation: _If a process delivers a message m with sender s, then m was previously broadcast by process s._
/// 4.  Agreement: _If a message m is delivered by some correct process, then m is eventually delivered by every correct process._
/// 5.  **Causal delivery**: _For any message m1 that potentially caused a message m2, i.e., m1 → m2, no process delivers m2 unless it has already delivered m1._
/// 
/// HINT: The recommended algorithm to use in this assignment is _Waiting Causal Broadcast_ and is described in page 4 within the following [document](https://d37djvu3ytnwxt.cloudfront.net/assets/courseware/v1/f11d45d4cb0d9685c723dd00de427b8d/asset-v1:KTHx+ID2203.1x+2016T3+type@asset+block/causalbroadcast.pdf) in the respective lecture.
/// 
/// **Also mind, that to complete this part, you will first have to implement and test Best-Effort Broadcast and Reliable Broadcast, defined above.**
/// 
/// #### Working with Vector Clocks
/// 
/// We have already provided you a `VectorClock` data structure **(already imported)** to aid you with the algorithm implementation. You can briefly see the supported operations below:
/// 
/// ```
///  case class VectorClock(var vc: Map[Address, Int]) {
///   def inc(addr: Address) : Unit  //increases the clock corresponding to the address @addr provided
///   def set(addr: Address, value: Int) : Unit //sets the clock of @addr to @value
///   def <=(that: VectorClock): Boolean   //returns true if this vector clock instance is lower or equal to @that
///  }
///  object VectorClock {
///    def empty(topology: scala.Seq[Address]): VectorClock //generates a vector clock that has an initial clock value of 0 for each address in the @topology provided
///    def apply(that: VectorClock): VectorClock //copy constructor of a vector clock. E.g. if vc1 is a vector clock vc2 = VectorClock(vc1) is a copy of vc1
///  }
/// ```
/// 
/// In case you want to check the full implementation of the VectorClock, you can find it in our full published gist [here](https://gist.github.com/senorcarbone/5c960ee27a67ec8b6bd42c33303fdcd2).
class WaitingCRB(init: Init[WaitingCRB]) extends ComponentDefinition {

  //WaitingCRB Subscriptions
  val rb = requires[ReliableBroadcast];
  val crb = provides[CausalOrderReliableBroadcast];

  //WaitingCRB Component State and Initialization
  val (self, vec) = init match {
    case Init(s: Address, t: Set[Address] @unchecked) => (s, VectorClock.empty(t.toSeq))
  };

  //  val V = VectorClock.empty(init match { case Init(_, t: Set[Address]) => t.toSeq })
  var pending: ListBuffer[(Address, DataMessage)] = ListBuffer();
  var lsn = 0;

  //WaitingCRB Event Handlers
  crb uponEvent {
    case x: CRB_Broadcast => {
        /* WRITE YOUR CODE HERE */
        var W = VectorClock(vec);
        W.set(self, lsn);
        lsn = lsn + 1;
        trigger(RB_Broadcast((DataMessage(W, x.payload))) -> rb);
    }
  }

  rb uponEvent {
    case x @ RB_Deliver(src: Address, msg: DataMessage) => {
        /* WRITE YOUR CODE HERE */
        pending += ((src, msg));
        while(pending.exists { p => p._2.timestamp<=vec }){
          for (p <- pending) {
            if (p._2.timestamp<=vec) {
              val (sender,dataMessage) = p;
              pending -= p;
              vec.inc(sender);
              trigger(CRB_Deliver(sender,dataMessage.payload) -> crb);
          }
        }
      }
    }
  }
}

object BroadcastCheck extends App  {
    // NOTE: this exercise has 3 parts, during development feel free to comment out individual checks.
    // For submission, all checks need to pass.
    checkBEB[BasicBroadcast]();
    checkRB[BasicBroadcast,EagerReliableBroadcast]();
    checkCRB[BasicBroadcast, EagerReliableBroadcast, WaitingCRB]();
}