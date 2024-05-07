//  ### Implementing an Atomic N-N register
// In this programming assignment you will have to complete the implementation of an atomic register that supports multiple writers and readers.

// When you are done you simply have to export your notebook and then upload it in the “Programming Exercise 3” page.

// **Things to Remember**:
// 1. Basic components such as PerfectLink, Network and Timer are already provided. No need to implement them.
// 2. Execute the imports defined below before compiling your component implementations.
// 3. We recommend making use of the component state and internal messages we have provided, if any, to complete the implementation logic.
// 4. You can always print messages to the output log, from within handlers to see what happens during the simulation. e.g. println(s"Process $self delivers message $msg");
// 5. Remember that during the simulation check you can print and observe the simulation time, i.e. with System.currentTimeMillis().
// 5. Do not forget to run the checker code block after each component implementation to ensure that all properties are satisfied before exporting and submitting the notebook.
// 6. You can always restart the Kompics Interpreter to start fresh (Interpreter→KompicsInterpreter→Click Restart)

// Good luck! :)

package se.kth.edx.id2203.templates

import se.kth.edx.id2203.core.ExercisePrimitives._
import se.kth.edx.id2203.core.Ports._
import se.kth.edx.id2203.templates.ReadImposeWriteConsultMajority._
import se.kth.edx.id2203.validation._
import se.sics.kompics.network._
import se.sics.kompics.sl.{ Init, _ }
import se.sics.kompics.{ ComponentDefinition => _, Port => _, _ }

import scala.collection.mutable.Map
import scala.language.implicitConversions

// ### Mini Scala-Refresher : The Scala Option
// In this assignment you will have to work with Option types, so let’s recap how they work. An Option[T] is a container for a value of type T which is optional.
// That means that in case the value has been set, then Option[T] is actually an instance of Some[T], which in turn contains the actual value of type T.
// In case its value has not been set then the Option[A] is the object None.

// Thus, it is good to have in mind the following e.g. for any value: Option[Any]:

// 1.When you want to initialize your optional value do it as such:
// value = Some(myInitialValue)
// or
// value = None;

// 2.In case you need to provide a default value in case of a None when you want to access an optional type you can do so like this:
// val myVal:T = value.getOrElse(myDefaultValue)

// Otherwise you can always simply get its value when you are sure it is not None like this:
// if(value.isDefined){
//     myVal = value.get
// }

// 3.Beware of the following assignment: for val myVal :Option[Any] = Some(1334) then val myOtherVal : Option[Any] = Some(myVal) will not be Some(1334) but Some(Some(1334))

// ### The N-N Atomic Register
// A (single) Atomic Register Abstraction (AR), in Kompics terms, is a component that provides the following port (already imported in the notebook).
// class AtomicRegister extends Port {
//   request[AR_Read_Request]
//   request[AR_Write_Request]
//   indication[AR_Read_Response]
//   indication[AR_Write_Response]
// }

// An AR component should request reads (AR_Read_Request) or writes (AR_Write_Request) and respond with AR_Read_Response or AR_Write_Response events respectively as defined below:
// case class AR_Read_Request() extends KompicsEvent
// case class AR_Read_Response(value: Option[Any]) extends KompicsEvent
// case class AR_Write_Request(value: Any) extends KompicsEvent
// case class AR_Write_Response() extends KompicsEvent

// As you have already learnt from the course lectures, Atomic Registers should be linerarizable and also terminate which we summarize with the following properties:

// Termination: If a correct process invokes an operation, then the operation eventually completes.
// Atomicity: Every read operation returns the value that was written most recently in a hypothetical execution, where every failed operation appears to be complete or does not appear to have been invoked at all, and every complete operation appears to have been executed at some instant between its invocation and its completion.
// HINT: The recommended algorithm to use in this assignment is Read-Impose Write-Consult Majority and is described at page 3 within the following document in the respective lecture.

object ReadImposeWriteConsultMajority {

  //events for internal use
  case class READ(rid: Int) extends KompicsEvent;
  case class VALUE(rid: Int, ts: Int, wr: Int, value: Option[Any]) extends KompicsEvent;
  case class WRITE(rid: Int, ts: Int, wr: Int, writeVal: Option[Any]) extends KompicsEvent;
  case class ACK(rid: Int) extends KompicsEvent;

  /**
   * This augments tuples with comparison operators implicitly, which you can use in your code.
   * examples: (1,2) > (1,4) yields 'false' and  (5,4) <= (7,4) yields 'true'
   */
  implicit def addComparators[A](x: A)(implicit o: math.Ordering[A]): o.OrderingOps = o.mkOrderingOps(x);

}

class ReadImposeWriteConsultMajority(init: Init[ReadImposeWriteConsultMajority]) extends ComponentDefinition {

  //subscriptions

  val nnar = provides[AtomicRegister];

  val pLink = requires[PerfectLink];
  val beb = requires[BestEffortBroadcast];

  //state and initialization

  val (self: Address, n: Int, selfRank: Int) = init match {
    case Init(selfAddr: Address, n: Int) => (selfAddr, n, AddressUtils.toRank(selfAddr))
  };

  var (ts, wr) = (0, 0);
  var value: Option[Any] = None;
  var acks = 0;
  var readval: Option[Any] = None;
  var writeval: Option[Any] = None;
  var rid = 0;
  var readlist: Map[Address, (Int, Int, Option[Any])] = Map.empty
  var reading = false;

  // handlers

  nnar uponEvent {
    case AR_Read_Request() => {
      rid = rid + 1;
      /* WRITE YOUR CODE HERE  */
      acks=0;
      readlist= Map.empty;
      reading = true;
      trigger(BEB_Broadcast(READ(rid))-> beb);
    };
    case AR_Write_Request(wval) => {
      rid = rid + 1;
      /* WRITE YOUR CODE HERE  */
      writeval= Some(wval);
      acks=0;
      readlist= Map.empty;
      trigger(BEB_Broadcast(READ(rid))-> beb);
    }
  }

  beb uponEvent {
    case BEB_Deliver(src, READ(readID)) => {
      /* WRITE YOUR CODE HERE  */
      trigger(PL_Send(src,VALUE(readID, ts, wr, value))->pLink);
    }
    case BEB_Deliver(src, w: WRITE) => {
      /* WRITE YOUR CODE HERE  */
      if((w.ts,w.wr)>(ts,wr)){
        ts=w.ts;
        wr=w.wr;
        value=w.writeVal;
      }
      trigger(PL_Send(src, ACK(w.rid))->pLink);
    }
  }

  pLink uponEvent {
    case PL_Deliver(src, v: VALUE) => {
      if (v.rid == rid) {
        /* WRITE YOUR CODE HERE  */
        readlist(src)=(v.ts, v.wr, v.value);
        if(readlist.size > n/2){
          var bcastval=None: Option[Any];
          var maxts = 0;
          var rr= 0;

          for ((k,vprime) <- readlist){
            if((vprime._1,vprime._2) > (maxts, rr)){
              maxts=vprime._1;
              rr = vprime._2;
              readval= vprime._3;
            }
          }
          readval = readlist.maxBy(maplist => (maplist._2._1, maplist._2._2))._2._3;
          println(maxts);
          println(rr);
          println(readval);
          readlist= Map.empty;
          if (reading==true){
            bcastval= readval;
          } else{
            rr=selfRank;
            maxts= maxts+1;
            bcastval=writeval;
          }
          trigger(BEB_Broadcast(WRITE(rid,maxts,rr,bcastval))->beb);
        } 
      }
    }
    case PL_Deliver(src, v: ACK) => {
      if (v.rid == rid) {
        /* WRITE YOUR CODE HERE  */
        acks= acks+1;
        if(acks>n/2){
          acks=0;
          if(reading==true){
            reading=false;
            trigger(AR_Read_Response(readval)->nnar);
          } else{
            trigger(AR_Write_Response()->nnar);
          }
        }
      }
    }
  }
}

object SharedMemoryCheck extends App {
  checkNNAR[ReadImposeWriteConsultMajority]();
}
