package net.corda.nodeapi.internal

import net.corda.core.identity.CordaX500Name
import net.corda.core.serialization.CordaSerializable
import net.corda.core.utilities.NetworkHostAndPort

@CordaSerializable
data class BridgeEntry(val queueName: String, val targets: List<NetworkHostAndPort>, val legalNames: List<CordaX500Name>)

sealed class BridgeControl {
    @CordaSerializable
    data class NodeToBridgeSnapshot(val inboxQueues: List<String>, val sendQueues: List<BridgeEntry>) : BridgeControl()

    @CordaSerializable
    data class BridgeToNodeQuery(val bridgeIdentity: String) : BridgeControl()

    @CordaSerializable
    data class BridgeCreate(val bridgeInfo: BridgeEntry) : BridgeControl()

    @CordaSerializable
    data class BridgeDelete(val bridgeInfo: BridgeEntry) : BridgeControl()
}