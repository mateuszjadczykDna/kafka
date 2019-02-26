package kafka.raft

import java.util
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import kafka.server.MetadataCache
import org.apache.kafka.clients.{ClientRequest, ClientResponse, KafkaClient}
import org.apache.kafka.common.message._
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.protocol.{ApiKeys, ApiMessage, Errors}
import org.apache.kafka.common.raft.{NetworkChannel, RaftMessage, RaftRequest, RaftResponse}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._
import scala.collection.mutable

class KafkaNetworkChannel(time: Time,
                          client: KafkaClient,
                          clientId: String,
                          retryBackoffMs: Int,
                          requestTimeoutMs: Int,
                          interBrokerListenerName: ListenerName,
                          metadata: MetadataCache) extends NetworkChannel {
  type ResponseHandler = AbstractResponse => Unit

  private val requestIdCounter = new AtomicInteger(0)
  private val pendingInbound = mutable.Map.empty[Long, ResponseHandler]
  private val undelivered = new ArrayBlockingQueue[RaftMessage](10)
  private val pendingOutbound = new ArrayBlockingQueue[RaftRequest.Outbound](10)

  override def newRequestId(): Int = requestIdCounter.getAndIncrement()

  private def buildResponse(responseData: ApiMessage): AbstractResponse = {
    responseData match {
      case voteResponse: VoteResponseData =>
        new VoteResponse(voteResponse)
      case beginEpochResponse: BeginEpochResponseData =>
        new BeginEpochResponse(beginEpochResponse)
      case endEpochResponse: EndEpochResponseData =>
        new EndEpochResponse(endEpochResponse)
      case fetchRecordsResponse: FetchRecordsResponseData =>
        new FetchRecordsResponse(fetchRecordsResponse)
      case fetchEndOffsetResponse: FetchEndOffsetResponseData =>
        new FetchEndOffsetResponse(fetchEndOffsetResponse)
      case findLeaderResponse: FindLeaderResponseData =>
        new FindLeaderResponse(findLeaderResponse)
    }
  }

  private def buildRequest(requestData: ApiMessage): AbstractRequest.Builder[_ <: AbstractRequest] = {
    requestData match {
      case voteRequest: VoteRequestData =>
        new VoteRequest.Builder(voteRequest)
      case beginEpochRequest: BeginEpochRequestData =>
        new BeginEpochRequest.Builder(beginEpochRequest)
      case endEpochRequest: EndEpochRequestData =>
        new EndEpochRequest.Builder(endEpochRequest)
      case fetchRecordsRequest: FetchRecordsRequestData =>
        new FetchRecordsRequest.Builder(fetchRecordsRequest)
      case fetchEndOffsetRequest: FetchEndOffsetRequestData =>
        new FetchEndOffsetRequest.Builder(fetchEndOffsetRequest)
      case findLeaderRequest: FindLeaderRequestData =>
        new FindLeaderRequest.Builder(findLeaderRequest)
    }
  }

  private def buildClientRequest(req: RaftRequest.Outbound): ClientRequest = {
    val destination = req.destinationId.toString
    val request = buildRequest(req.data)
    val correlationId = req.requestId
    val createdTimeMs = req.createdTimeMs
    new ClientRequest(destination, request, correlationId, clientId, createdTimeMs, true,
      requestTimeoutMs, null)
  }

  override def send(message: RaftMessage): Unit = {
    message match {
      case request: RaftRequest.Outbound =>
        pendingOutbound.offer(request)

      case response: RaftResponse.Outbound =>
        pendingInbound.remove(response.requestId).foreach { onResponseReceived: ResponseHandler =>
          onResponseReceived(buildResponse(response.data))
        }
      case _ =>
        throw new IllegalArgumentException("Unhandled message type " + message)
    }
  }

  private def sendOutboundRequests(currentTimeMs: Long): Unit = {
    while (!pendingOutbound.isEmpty) {
      val request = pendingOutbound.peek()

      metadata.brokerIfAlive(request.destinationId) match {
        case Some(broker) =>
          val node = broker.node(interBrokerListenerName)
          if (client.isReady(node, currentTimeMs)) {
            pendingOutbound.poll()
            val clientRequest = buildClientRequest(request)
            client.send(clientRequest, currentTimeMs)
          } else {
            // We will retry this request on the next poll
            return
          }

        case None =>
          pendingOutbound.poll()
          val responseData = errorResponseData(apiKeyForRequest(request), Errors.BROKER_NOT_AVAILABLE)
          undelivered.offer(new RaftResponse.Inbound(request.requestId, responseData, request.destinationId))
      }
    }
  }

  private def apiKeyForRequest(request: RaftRequest): ApiKeys = {
    // TODO: could probably push ApiKey into RaftRequest to make this annoying method go away
    request.data match {
      case _: VoteRequestData => ApiKeys.VOTE
      case _: BeginEpochRequestData => ApiKeys.BEGIN_EPOCH
      case _: EndEpochRequestData => ApiKeys.END_EPOCH
      case _: FetchRecordsRequestData => ApiKeys.FETCH_RECORDS
      case _: FetchEndOffsetRequestData => ApiKeys.FETCH_END_OFFSET
      case _: FindLeaderRequestData => ApiKeys.FIND_LEADER
      case _ => throw new IllegalArgumentException(s"Unknown request type ${request.data}")
    }
  }

  private def errorResponseData(apiKey: ApiKeys, error: Errors): ApiMessage = {
    apiKey match {
      case ApiKeys.VOTE =>
        new VoteResponseData()
          .setErrorCode(error.code)
          .setVoteGranted(false)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case ApiKeys.BEGIN_EPOCH =>
        new BeginEpochResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case ApiKeys.END_EPOCH =>
        new EndEpochResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case ApiKeys.FETCH_RECORDS =>
        new FetchRecordsResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
          .setHighWatermark(-1)
          .setRecords(new Array[Byte](0))
      case ApiKeys.FETCH_END_OFFSET =>
        new FetchEndOffsetResponseData()
          .setErrorCode(error.code)
          .setEndOffset(-1)
          .setEndOffsetEpoch(-1)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case ApiKeys.FIND_LEADER =>
        new FindLeaderResponseData()
          .setErrorCode(error.code)
          .setLeaderEpoch(-1)
          .setLeaderId(-1)
      case _ =>
        throw new IllegalArgumentException(s"Received response for unexpected request type: $apiKey")
    }
  }

  private def buildInboundRaftResponse(response: ClientResponse): RaftResponse.Inbound = {
    val header = response.requestHeader()
    val data = if (response.wasDisconnected) {
      errorResponseData(header.apiKey, Errors.BROKER_NOT_AVAILABLE)
    } else if (response.authenticationException != null) {
      errorResponseData(header.apiKey, Errors.CLUSTER_AUTHORIZATION_FAILED)
    } else {
      responseData(response.responseBody)
    }
    new RaftResponse.Inbound(header.correlationId, data, response.destination.toInt)
  }

  private def responseData(response: AbstractResponse): ApiMessage = {
    response match {
      case voteResponse: VoteResponse => voteResponse.data
      case beginEpochResponse: BeginEpochResponse => beginEpochResponse.data
      case endEpochResponse: EndEpochResponse => endEpochResponse.data
      case fetchRecordsResponse: FetchRecordsResponse => fetchRecordsResponse.data
      case fetchEndOffsetResponse: FetchEndOffsetResponse => fetchEndOffsetResponse.data
      case findLeaderResponse: FindLeaderResponse => findLeaderResponse.data
    }
  }

  private def requestData(request: AbstractRequest): ApiMessage = {
    request match {
      case voteRequest: VoteRequest => voteRequest.data
      case beginEpochRequest: BeginEpochRequest => beginEpochRequest.data
      case endEpochRequest: EndEpochRequest => endEpochRequest.data
      case fetchRecordsRequest: FetchRecordsRequest => fetchRecordsRequest.data
      case fetchEndOffsetRequest: FetchEndOffsetRequest => fetchEndOffsetRequest.data
      case findLeaderRequest: FindLeaderRequest => findLeaderRequest.data
    }
  }

  private def pollInboundResponses(timeoutMs: Long): util.List[RaftMessage] = {
    val pollTimeoutMs = if (!undelivered.isEmpty) {
      0L
    } else if (!pendingOutbound.isEmpty) {
      retryBackoffMs
    } else {
      timeoutMs
    }
    val responses = client.poll(pollTimeoutMs, time.milliseconds())
    val messages = new util.ArrayList[RaftMessage]
    for (response <- responses.asScala) {
      messages.add(buildInboundRaftResponse(response))
    }
    undelivered.drainTo(messages)
    messages
  }

  override def receive(timeoutMs: Long): util.List[RaftMessage] = {
    sendOutboundRequests(time.milliseconds())
    pollInboundResponses(timeoutMs)
  }

  override def wakeup(): Unit = {
    client.wakeup()
  }

  def postInboundRequest(request: AbstractRequest, onResponseReceived: ResponseHandler): Unit = {
    val data = requestData(request)
    val requestId = newRequestId()
    val req = new RaftRequest.Inbound(requestId, data, time.milliseconds())
    pendingInbound.put(requestId, onResponseReceived)
    undelivered.offer(req)
    wakeup()
  }

}
