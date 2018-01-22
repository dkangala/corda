package net.corda.node.services.messaging

import net.corda.core.serialization.SerializationDefaults
import net.corda.core.serialization.deserialize
import net.corda.core.serialization.serialize
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.contextLogger
import net.corda.node.services.config.NodeConfiguration
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.BRIDGE_CONTROL
import net.corda.nodeapi.internal.ArtemisMessagingComponent.Companion.BRIDGE_NOTIFY
import net.corda.nodeapi.internal.BridgeControl
import org.apache.activemq.artemis.api.core.RoutingType
import org.apache.activemq.artemis.api.core.client.ClientConsumer
import org.apache.activemq.artemis.api.core.client.ClientMessage
import java.util.*

internal class BridgeControlListener(val config: NodeConfiguration,
                                     val p2pAddress: NetworkHostAndPort,
                                     val maxMessageSize: Int) : AutoCloseable {
    private val bridgeId: String = UUID.randomUUID().toString()
    private val bridgeManager: BridgeManager = AMQPBridgeManager(config, p2pAddress, maxMessageSize)
    private var artemis: ArtemisMessagingClient? = null
    private var controlConsumer: ClientConsumer? = null

    companion object {
        private val log = contextLogger()
    }

    fun start() {
        stop()
        bridgeManager.start()
        val artemis = ArtemisMessagingClient(config, p2pAddress, maxMessageSize)
        this.artemis = artemis
        artemis.start()
        val artemisClient = artemis.started!!
        val artemisSession = artemisClient.session
        val bridgeControlQueue = "$BRIDGE_CONTROL.$bridgeId"
        artemisSession.createTemporaryQueue(BRIDGE_CONTROL, RoutingType.MULTICAST, bridgeControlQueue)
        val control = artemisSession.createConsumer(bridgeControlQueue)
        controlConsumer = control
        control.setMessageHandler { msg ->
            try {
                processControlMessage(msg)
            } catch (ex: Exception) {
                log.error("Unable to process bridge control message", ex)
            }
        }
        val startupMessage = BridgeControl.BridgeToNodeQuery("Test").serialize(context = SerializationDefaults.P2P_CONTEXT).bytes
        val bridgeRequest = artemisSession.createMessage(false)
        bridgeRequest.writeBodyBufferBytes(startupMessage)
        artemisClient.producer.send(BRIDGE_NOTIFY, bridgeRequest)
    }

    fun stop() {
        controlConsumer?.close()
        controlConsumer = null
        artemis?.stop()
        artemis = null
        bridgeManager.stop()
    }

    override fun close() = stop()

    private fun processControlMessage(msg: ClientMessage) {
        val data: ByteArray = ByteArray(msg.bodySize).apply { msg.bodyBuffer.readBytes(this) }
        val controlMessage = data.deserialize<BridgeControl>(context = SerializationDefaults.P2P_CONTEXT)
        log.info(controlMessage.toString())
        when (controlMessage) {
            is BridgeControl.NodeToBridgeSnapshot -> {
                for (outQueue in controlMessage.sendQueues) {
                    bridgeManager.deployBridge(outQueue.queueName, outQueue.targets.first(), outQueue.legalNames.toSet())
                }
            }
            is BridgeControl.BridgeToNodeQuery -> {
                log.warn("Message from Bridge $controlMessage detected on wrong topic!")
            }
            is BridgeControl.BridgeCreate -> {
                bridgeManager.deployBridge(controlMessage.bridgeInfo.queueName, controlMessage.bridgeInfo.targets.first(), controlMessage.bridgeInfo.legalNames.toSet())
            }
            is BridgeControl.BridgeDelete -> {
                bridgeManager.destroyBridge(controlMessage.bridgeInfo.queueName, controlMessage.bridgeInfo.targets.first())
            }
        }
    }

}