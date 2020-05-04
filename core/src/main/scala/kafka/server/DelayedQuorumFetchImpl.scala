package kafka.server

import org.apache.kafka.purgatory.DelayedQuorumFetch

trait DelayedQuorumFetchTrait extends DelayedQuorumFetch {

}

class DelayedQuorumFetchImpl(delayMs: Long) extends DelayedOperationImpl(delayMs) with DelayedQuorumFetchTrait {
  
}
