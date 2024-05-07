// ### Distributed Consensus
// In this final programming assignment for Part I of the course you will have to complete the implementation of a variation of the famous Paxos algorithm.

// When you are done you simply have to export your notebook and then upload it in the “Programming Exercise 5” page.

// Things to Remember:
// 1. Basic components such as PerfectLink and Best-Effort Broadcast are already provided. No need to implement them.
// 2. Execute the imports defined below before compiling your component implementations.
// 3. We recommend making use of the component state and internal messages we have provided, if any, to complete the implementation logic.
// 4. You can always print messages to the output log, from within handlers to see what happens during the simulation. e.g. println(s"Process $self delivers message $msg");
// 5. Remember that during the simulation check you can print and observe the simulation time, i.e. with System.currentTimeMillis().
// 5. Do not forget to run the checker code block after each component implementation to ensure that all properties are satisfied before exporting and submitting the notebook.
// 6. You can always restart the Kompics Interpreter to start fresh (Interpreter→KompicsInterpreter→Click Restart)

// Good luck! :)

package se.kth.edx.id2203.templates

import se.kth.edx.id2203.core.ExercisePrimitives.AddressUtils._
import se.kth.edx.id2203.core.Ports._
import se.kth.edx.id2203.templates.Paxos._
import se.kth.edx.id2203.validation._
import se.sics.kompics.network._
import se.sics.kompics.sl.{ Init, _ }
import se.sics.kompics.{ KompicsEvent, ComponentDefinition => _, Port => _ }
import scala.language.implicitConversions
import scala.collection.mutable.ListBuffer;

// ### Leader-less Obstraction-Free Paxos for Single Value Consensus
// A (single value) Consensus Abstraction, in Kompics terms, is a component that provides the following port (already imported in the notebook).

//  class Consensus extends Port{
//    request[C_Propose];
//    indication[C_Decide];
//  }

// An Consensus component should request value proposals (C_Propose) and respond with decided value events (C_Decide) respectively as defined below:

//  case class C_Decide(value: Any) extends KompicsEvent;
//  case class C_Propose(value: Any) extends KompicsEvent;

// The following properties define the expected behavior of a consensus abstraction more specifically:

// 1. Validity: Only proposed values may be decided.
// 2. Uniform Agreement: No two nodes decide different values.
// 3. Integrity: Each node can decide a value at most once.
// 4. Termination: Every node eventually decides a value.


// The recommended algorithm to use is the the one we call “Leaderless Repeatable Paxos” which initiates new proposal rounds until a decision has been made.
// You can find the algorithm in the following document (pages 2-3): https://canvas.kth.se/courses/37639/files/6036181?wrap=1
object Paxos {

  case class Prepare(proposalBallot: (Int, Int)) extends KompicsEvent;
  case class Promise(promiseBallot: (Int, Int), acceptedBallot: (Int, Int), acceptedValue: Option[Any]) extends KompicsEvent;
  case class Accept(acceptBallot: (Int, Int), proposedValue: Any) extends KompicsEvent;
  case class Accepted(acceptedBallot: (Int, Int)) extends KompicsEvent;
  case class Nack(ballot: (Int, Int)) extends KompicsEvent;
  case class Decided(decidedValue: Any) extends KompicsEvent;

  /**
   * This augments tuples with comparison operators implicitly, which you can use in your code, for convenience.
   * examples: (1,2) > (1,4) yields 'false' and  (5,4) <= (7,4) yields 'true'
   */
  implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.OrderingOps = o.mkOrderingOps(x);
  //HINT: After you execute the latter implicit ordering you can compare tuples as such within your component implementation:
  // (1,2) <= (1,4);

}

class Paxos(paxosInit: Init[Paxos]) extends ComponentDefinition {

  //Port Subscriptions for Paxos

  val consensus = provides[Consensus];
  val beb = requires[BestEffortBroadcast];
  val plink = requires[PerfectLink];

  //Internal State of Paxos

  val (rank, numProcesses) = paxosInit match {
    case Init(s: Address, qSize: Int) => (toRank(s), qSize)
  }

  //Proposer State
  var round = 0;
  var proposedValue: Option[Any] = None;
  var promises: ListBuffer[((Int, Int), Option[Any])] = ListBuffer.empty;
  var numOfAccepts = 0;
  var decided = false;

  //Acceptor State
  var promisedBallot = (0, 0);
  var acceptedBallot = (0, 0);
  var acceptedValue: Option[Any] = None;

  def propose() = {
   /* 
   INSERT YOUR CODE HERE 
   */
   if(decided==false){
    round+=1;
    numOfAccepts=0;
    promises=ListBuffer.empty;
    trigger(BEB_Broadcast(Prepare((round, rank)))->beb);
    }
  }

  consensus uponEvent {
    case C_Propose(value) => {
     /* 
     INSERT YOUR CODE HERE 
     */
     proposedValue = Some(value);
     propose();
    }
  }

  beb uponEvent {

    case BEB_Deliver(src, prep: Prepare) => {
     /* 
     INSERT YOUR CODE HERE 
     */
     if(promisedBallot<prep.proposalBallot){
      promisedBallot=prep.proposalBallot;
      trigger(PL_Send(src,Promise(promisedBallot, acceptedBallot, acceptedValue))->plink);
     } else{
      trigger(PL_Send(src,Nack(prep.proposalBallot))->plink)
     }
    };

    case BEB_Deliver(src, acc: Accept) => {
     /* 
     INSERT YOUR CODE HERE 
     */
     if(promisedBallot<=acc.acceptBallot){
      promisedBallot=acc.acceptBallot;
      acceptedBallot=acc.acceptBallot;
      acceptedValue=Some(acc.proposedValue);
      trigger(PL_Send(src,Accepted(acc.acceptBallot))->plink);
      } else{
        trigger(PL_Send(src,Nack(acc.acceptBallot))->plink);
      }
    };

    case BEB_Deliver(src, dec: Decided) => {
     /* 
     INSERT YOUR CODE HERE 
     */
     if(decided==false){
      trigger(C_Decide(dec.decidedValue)->consensus);
      decided=true;
      }
    }
  }

  plink uponEvent {

    case PL_Deliver(src, prepAck: Promise) => {
      if ((round, rank) == prepAck.promiseBallot) {
       /* 
       INSERT YOUR CODE HERE 
       */
       promises+=((prepAck.acceptedBallot,prepAck.acceptedValue));
       if(promises.length == (1+(numProcesses)/2)){
        println("Promises received: ",promises);
        val (maxBallot, value) =promises.maxBy { case (key, value) => key };
        if(value!=None){
          proposedValue=value;
        }
          trigger(BEB_Broadcast(Accept((round, rank), proposedValue.get))->beb);
        }
      }
    };

    case PL_Deliver(src, accAck: Accepted) => {
      if ((round, rank) == accAck.acceptedBallot) {
        /* 
           INSERT YOUR CODE HERE 
        */
        numOfAccepts = numOfAccepts +1;
        if(numOfAccepts==(1+(numProcesses)/2)){
          trigger(BEB_Broadcast(Decided(proposedValue.get))->beb);
        }
      }
    };

    case PL_Deliver(src, nack: Nack) => {
      if ((round, rank) == nack.ballot) {
        /* 
           INSERT YOUR CODE HERE 
        */
        propose();
      }
    }
  }

};

object PaxosTemplate extends App {
  checkSingleValueConsensus[Paxos]();
}