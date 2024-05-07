/// In this programming assignment you will have to complete the implementation of a Leader-Based Sequence Consensus.

/// Sequence Consensus abstraction, in Kompics terms, is a composite component that **provides** the following port *(already imported)*.

/// class SequenceConsensus extends Port 
/// { 
///  request[SC_Propose];
///  indication[SC_Decide];
/// }
/// In your implementation, the **Sequence Consensus** component should indicate with a `SC_Decide` message every value that is appended in the decided sequence (invoked in FIFO order) 
/// as well as requesting proposed values through a `SC_Propose` message:
/// case class SC_Propose(value: RSM_Command) extends KompicsEvent;
/// case class SC_Decide(value: RSM_Command) extends KompicsEvent;

/// Your sequence consensus component builds on top of a `Ballot Leader Election` component (simulated, no need to provide an implementation). 
/// That means that it subscribes to `BLE_Leader(Leader, Ballot)` indications whenever a new leader is elected alongside a maximum ballot. 
/// Furthermore, a `FIFOPerfectLink` abstraction is also provided to send and receive messages in FIFO order between every two processes, 
/// using the same messages as the PerfectLink port introduced in Part I (i.e., using `PL_Send(destination, payload)` and `PL_Receive(sender, payload)` messages)  
/// The following properties define the expected behavior of a consensus abstraction more specifically:
/// 1. **Validity**: *Every value decided in a sequence has been previously proposed*
/// 2. **Uniform Agreement**: *For every two sequences decided across different processs, one is a prefix of the other.*
/// 3. **Completeness**: *Every command proposed by a correct process is eventually included in a decided sequence by every correct process.*

/// The recommended algorithm to use is the the one we call \"Leader-Based Sequence Paxos\" which extends and optimises single value paxos to work with sequences of values.
/// You can find the algorithm in the following link https://courses.edx.org/asset-v1:KTHx+ID2203.2x+2016T4+type@asset+block@sequence-paxos.pdf



package se.kth.edx.id2203.templates

import se.kth.edx.id2203.core.ExercisePrimitives.AddressUtils
import se.sics.kompics.sl._
import se.sics.kompics.network._
import se.kth.edx.id2203.core.Ports.{SequenceConsensus, _}
import se.sics.kompics.KompicsEvent
import se.kth.edx.id2203.validation._

import collection.mutable

object SequencePaxos {
  case class Prepare(nL: Long, ld: Int, na: Long) extends KompicsEvent;
  case class Promise(nL: Long, na: Long, suffix: List[RSM_Command], ld: Int) extends KompicsEvent;
  case class AcceptSync(nL: Long, suffix: List[RSM_Command], ld: Int) extends KompicsEvent;
  case class Accept(nL: Long, c: RSM_Command) extends KompicsEvent;
  case class Accepted(nL: Long, m: Int) extends KompicsEvent;
  case class Decide(ld: Int, nL: Long) extends KompicsEvent;

  object State extends Enumeration {
    type State = Value;
    val PREPARE, ACCEPT, UNKOWN = Value;
  }

  object Role extends Enumeration {
    type Role = Value;
    val LEADER, FOLLOWER = Value;
  }
}

class SequencePaxos(init: Init[SequencePaxos]) extends ComponentDefinition {
  import SequencePaxos._
  import State._
  import Role._

  val sc = provides[SequenceConsensus];
  val ble = requires[BallotLeaderElection];
  val pl = requires[FIFOPerfectLink];

  val (self, pi, others) = init match {
    case Init(addr: Address, pi: Set[Address] @unchecked) => (addr, pi, pi - addr)
  }
  val majority = (pi.size / 2) + 1;

  var state = (FOLLOWER, UNKOWN);
  var nL = 0l;
  var nProm = 0l;
  var leader: Option[Address] = None;
  var na = 0l;
  var va = List.empty[RSM_Command];
  var ld = 0;
  // leader state
  var propCmds = List.empty[RSM_Command];
  val las = mutable.Map.empty[Address, Int];
  val lds = mutable.Map.empty[Address, Int];
  var lc = 0;
  val acks = mutable.Map.empty[Address, (Long, List[RSM_Command])];

  private def suffix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
    s.drop(l)
  }

  private def prefix(s: List[RSM_Command], l: Int): List[RSM_Command] = {
    s.take(l)
  }

  ble uponEvent {
    case BLE_Leader(l, n) => {
        /* INSERT YOUR CODE HERE */
      if(n>nL){
        leader=Option(l);
        nL=n;
        if(l==self && nL>nProm){
          state=(LEADER,PREPARE);
          propCmds = List.empty[RSM_Command];
          for (p <- pi) {
            las(p) = 0;
          }
          for(p<-others){
            trigger(PL_Send(p,Prepare(nL, ld, na))->pl);
          }
          acks(l)=(na, suffix(va,ld));
          lds(self)=ld;
          nProm=nL;
        } else{
          state=(FOLLOWER,state._2);
        }
      }
    }
  }

  pl uponEvent {
    case PL_Deliver(p, Prepare(np, ldp, n)) => {
        /* INSERT YOUR CODE HERE */
      if(nProm<np){
        nProm=np;
        state=(FOLLOWER,PREPARE);
        var sfx = List.empty[RSM_Command];
        if(na>=n){
          sfx=suffix(va, ld)
        }
        trigger(PL_Send(p,Promise(np, na, sfx,ld))->pl)
      }
    }
    case PL_Deliver(a, Promise(n, na, sfxa, lda)) => {
      if ((n == nL) && (state == (LEADER, PREPARE))) {
        /* INSERT YOUR CODE HERE */
        acks(a)=(na, sfxa);
        lds(a)=lda;

        if (acks.size >= majority) {
          var sfx = List.empty[RSM_Command];
          var maxRound = 0l;

          for (ack <- acks) {
            if ((ack._2._1 > maxRound)) {
              maxRound = ack._2._1;
              sfx = ack._2._2;
            }
          }
          va = prefix(va, ld) ++ sfx ++ propCmds;
          las(self) = va.size;
          propCmds = List.empty[RSM_Command];
          state = (LEADER, ACCEPT);
          for (p <- pi) {
            if (lds.contains(p) && p != self) {
              var sfxp = suffix(va, lds(p));
              trigger(PL_Send(p, AcceptSync(nL, sfxp, lds(p))) -> pl);
            }
          }
        }
      } else if ((n == nL) && (state == (LEADER, ACCEPT))) {
        /* INSERT YOUR CODE HERE */
        lds(a)=lda;
        var sfx=suffix(va, lds(a));
        trigger(PL_Send(a, AcceptSync(nL, sfx, lds(a))) -> pl);
        if(lc!=0){
          trigger(PL_Send(a, Decide(ld, nL))->pl)
        }
      }
    }
    case PL_Deliver(p, AcceptSync(nL, sfx, ldp)) => {
      if ((nProm == nL) && (state == (FOLLOWER, PREPARE))) {
        /* INSERT YOUR CODE HERE */
        if(nProm==nL){
          na=nL;
          va=prefix(va, ld)++sfx;
          trigger(PL_Send(p, Accepted(nL, va.size))->pl);
          state=(FOLLOWER, ACCEPT);
        }
      }
    }
    case PL_Deliver(p, Accept(nL, c)) => {
      if ((nProm == nL) && (state == (FOLLOWER, ACCEPT))) {
        /* INSERT YOUR CODE HERE */
        if(nProm==nL){
          va=va:::List(c);
          trigger(PL_Send(p, Accepted(nL,va.size))->pl);

        }
      }
    }
    case PL_Deliver(_, Decide(l, nL)) => {
        /* INSERT YOUR CODE HERE */
      if(nProm==nL){
        while(ld<l){
          trigger(SC_Decide(va(ld)) -> sc);
          ld=ld+1;
        }
      }
    }
    case PL_Deliver(a, Accepted(n, m)) => {
      if ((n == nL) && (state == (LEADER, ACCEPT))) {
        /* INSERT YOUR CODE HERE */
        las(a)=m;
        var P= Set.empty[Address];
        for(p<-pi){
          if(las(p)>=m){
            P=P+p;
          }
        }
        if (lc < m && P.size  >= majority) {
          lc = m;
          var nonEmptyLdsP= Set.empty[Address];
          for(q<-pi){
            if(lds.contains(q)){
              nonEmptyLdsP +=q;
            }
          }
          for (r <-nonEmptyLdsP ) {
            trigger(PL_Send(r, Decide(lc, nL)) -> pl);
          }
        }
      }
    }
  }

  sc uponEvent {
    case SC_Propose(c) => {
      if (state == (LEADER, PREPARE)) {
        /* INSERT YOUR CODE HERE */
        propCmds=propCmds:::List(c);
      }
      else if (state == (LEADER, ACCEPT)) {
        /* INSERT YOUR CODE HERE */
        va=va ::: List(c);
        las(self)=las(self)+1;
        for(p<-others){
          if(lds.contains(p) && !p.equals(self)){
            trigger(PL_Send(p,Accept(nL, c))->pl);
          }
        }
      }
    }
  }
}



object SeqConsensus extends App {
  checkSeqConsensus[SequencePaxos]();
}