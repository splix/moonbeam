package io.emeraldpay.polkadotcrawler.libp2p

import com.google.protobuf.ByteString
import com.southernstorm.noise.protocol.CipherState
import com.southernstorm.noise.protocol.DHState
import com.southernstorm.noise.protocol.HandshakeState
import com.southernstorm.noise.protocol.Noise
import io.libp2p.core.PeerId
import io.libp2p.core.crypto.PrivKey
import io.libp2p.core.crypto.marshalPublicKey
import io.libp2p.core.crypto.unmarshalPublicKey
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spipe.pb.Spipe
import java.nio.ByteBuffer
import java.util.concurrent.CompletableFuture
import java.util.function.Function

/**
 * Lib2P2 secure transport using Noise Protocol
 */
class NoiseTransport(
        /**
         * local peer private key
         */
        private val localKey: PrivKey,
        /**
         * true if this peer is initiating connection
         */
        private val initiator: Boolean
) {

    companion object {
        private val log = LoggerFactory.getLogger(NoiseTransport::class.java)

        @JvmStatic
        var localStaticPrivateKey25519: ByteArray = ByteArray(32).also { Noise.random(it) }
        private const val MAC_LENGTH = 16

        private val framer = SizePrefixed.Twobytes()
    }

    private val localNoiseState = Noise.createDH("25519")
    private val handshakeState = HandshakeState(
            "Noise_IX_25519_ChaChaPoly_SHA256",
            if (initiator) {HandshakeState.INITIATOR} else {HandshakeState.RESPONDER}
    )
    private var peerId: PeerId? = null
    private var senderKey: CipherState? = null
    private var receiverKey: CipherState? = null

    private val peerIdFuture = CompletableFuture<PeerId>()

    init {
        // configure the localDHState with the private key
        // which will automatically generate the corresponding public key
        localNoiseState.setPrivateKey(localStaticPrivateKey25519, 0)
        handshakeState.localKeyPair.copyFrom(localNoiseState)
        // start Noise protocol
        handshakeState.start()
    }

    fun handshake(): Publisher<ByteBuffer> {
        log.debug("Starting handshake")
        if (initiator) {
            log.debug("Initiate Noise protocol")
            // get identity public key
            val publicKey: ByteArray = marshalPublicKey(localKey.publicKey())

            // get noise static public key signature
            val signature = localKey.sign(getPublicKey(localNoiseState))

            // generate an appropriate protobuf element
            val payloadMsg =
                    Spipe.NoiseHandshakePayload.newBuilder()
                            .setLibp2PKey(ByteString.copyFrom(publicKey)) //pubkey
                            .setNoiseStaticKeySignature(ByteString.copyFrom(signature)) //signature
                            .build()
            val payload = framer.write(ByteBuffer.wrap(payloadMsg.toByteArray())).array()

            // create the message with the signed payload -
            // verification happens once the noise static key is shared
            return Mono.just(ByteBuffer.wrap(handshakeMessage(payload)))
        }
        return Mono.empty()
    }

    fun establish(inbound: Flux<ByteBuffer>): Publisher<ByteBuffer> {
        return inbound
                //read & decrypt message with size prefix
                .transform(framer.reader())
                .map {
                    val len = it.remaining()
                    val payload = ByteArray(len)
                    val read = handshakeState.readMessage(it.array(), 0, len, payload, 0)
                    ByteBuffer.wrap(payload.copyOfRange(0, read))
                }
                //the protobuf is also packed with size prefix
                .transform(framer.reader())
                //expecting just a single response
                .next()
                .map {
                    val msg = Spipe.NoiseHandshakePayload.parseFrom(it.array())
                    val publicKey = unmarshalPublicKey(msg.libp2PKey.toByteArray())
                    val signature = msg.noiseStaticKeySignature.toByteArray()

                    val valid = publicKey.verify(getPublicKey(handshakeState.remotePublicKey), signature)
                    log.debug("Noise verification result: $valid") //TODO fail if not valid?

                    peerId = PeerId.fromPubKey(publicKey)
                    peerIdFuture.complete(peerId)

                    //extract keys used for encryption/decryption
                    val cipherStatePair = handshakeState.split()
                    senderKey = cipherStatePair.sender
                    receiverKey = cipherStatePair.receiver
                }
                .doOnError {
                    log.warn("Failed to confirm Noise", it)
                    if (!peerIdFuture.isDone) {
                        peerIdFuture.completeExceptionally(it)
                    }
                }
                .then(Mono.empty())
    }

    /**
     * @return true when connection is successfully established
     */
    fun isEstablished(): Boolean {
        return peerId != null && senderKey != null && receiverKey != null
    }

    fun decoder(): Function<Flux<ByteBuffer>, Flux<ByteBuffer>> {
        return Function {
            it.map { ciphertext ->
                val plain = ByteArray(ciphertext.remaining())
                val lenPlain = receiverKey!!.decryptWithAd(null, ciphertext.array(), 0, plain, 0, ciphertext.remaining())
                ByteBuffer.wrap(plain, 0, lenPlain)
            }
        }
    }

    fun encoder(): Function<Flux<ByteBuffer>, Flux<ByteBuffer>> {
        return Function {
            it.map { plain ->
                val lenPlain = plain.remaining()
                val buf = ByteArray(lenPlain + senderKey!!.macLength)
                plain.get(buf, 0, lenPlain)
                val lenEncrypted = senderKey!!.encryptWithAd(null, buf, 0, buf, 0, lenPlain)
                ByteBuffer.wrap(buf, 0, lenEncrypted)
            }
        }
    }

    fun getPeerId(): Mono<PeerId> {
        if (peerId != null) {
            return Mono.just(peerId!!)
        }
        return Mono.fromFuture(peerIdFuture)
    }

    private fun handshakeMessage(msg: ByteArray): ByteArray {
        val msgLength = msg.size
        val outputBuffer = ByteArray(msgLength + (2 * (handshakeState.localKeyPair.publicKeyLength + MAC_LENGTH)))
        val outputLength = handshakeState.writeMessage(outputBuffer, 0, msg, 0, msgLength)
        return outputBuffer.copyOfRange(0, outputLength)
    }

    private fun getPublicKey(dhState: DHState): ByteArray {
        val pubKey = ByteArray(dhState.publicKeyLength)
        dhState.getPublicKey(pubKey, 0)
        return pubKey
    }

}