package io.emeraldpay.polkadotcrawler.libp2p

import io.emeraldpay.polkadotcrawler.DebugCommons
import io.libp2p.core.PeerId
import io.libp2p.core.crypto.PrivKey
import io.libp2p.core.crypto.PubKey
import io.libp2p.core.crypto.sha256
import io.libp2p.core.crypto.unmarshalPublicKey
import io.libp2p.crypto.keys.EcdsaPrivateKey
import io.libp2p.crypto.keys.EcdsaPublicKey
import io.libp2p.crypto.keys.decodeEcdsaPublicKeyUncompressed
import io.libp2p.crypto.keys.generateEcdsaKeyPair
import io.libp2p.crypto.stretchKeys
import io.libp2p.etc.types.compareTo
import io.libp2p.etc.types.toProtobuf
import io.libp2p.security.secio.*
import org.apache.commons.codec.binary.Hex
import org.bouncycastle.crypto.StreamCipher
import org.bouncycastle.crypto.digests.SHA256Digest
import org.bouncycastle.crypto.digests.SHA512Digest
import org.bouncycastle.crypto.macs.HMac
import org.bouncycastle.crypto.params.KeyParameter
import org.bouncycastle.jce.ECNamedCurveTable
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import spipe.pb.Spipe
import java.io.IOException
import java.nio.ByteBuffer
import java.security.SecureRandom

class Secio(
        val localKey: PrivKey
) {

    companion object {
        private val log = LoggerFactory.getLogger(Secio::class.java)
        private val ciphers = linkedSetOf("AES-128", "AES-256")
        private val hashes = linkedSetOf("SHA256", "SHA512")
        private val curves = linkedSetOf("P-256", "P-384", "P-521")

        private val random = SecureRandom()
        private val nonceSize = 16
    }

    private lateinit var proposeMsg: Spipe.Propose
    // proposed by a remote, the bot doesn't verify that it equal to a pubkey provided with address
    private lateinit var remotePubKey: PubKey
    private var order: Int? = null
    private lateinit var curve: String
    private lateinit var hash: String
    private lateinit var cipher: String
    private lateinit var ephPrivKey: EcdsaPrivateKey
    private lateinit var ephPubKey: EcdsaPublicKey

    private val localPubKeyBytes = localKey.publicKey().bytes()
    public val remoteNonce: ByteArray
        get() = remotePropose.rand.toByteArray()

    private lateinit var localParams: SecioParams
    private lateinit var remoteParams: SecioParams
    private lateinit var localCipher: StreamCipher
    private lateinit var remoteCipher: StreamCipher

    private var established = false

    private lateinit var remotePropose: Spipe.Propose

    val nonce = ByteArray(nonceSize).apply { random.nextBytes(this) }

    private val multistream: Multistream = Multistream()

    fun getPeerId(): PeerId {
        // actual peerid, it could be different from id provided with the address
        return PeerId.fromPubKey(remotePubKey)
    }

    fun isEstablished(): Boolean {
        return established
    }

    fun replyNonce(): Publisher<ByteBuffer> {
        return Mono.just(this)
                .map { ByteBuffer.wrap(it.remoteNonce) }
                .map(encoder())
                .doOnError { t -> log.error("Secio nonce reply failure", t) }
    }

    fun replyExchange(): Publisher<ByteBuffer> {
        return Mono.just(this)
                .map { ByteBuffer.wrap(it.buildExchange().toByteArray()) }
                .flux()
                .transform(SizePrefixed.Standard().writer())
                .doOnError { t -> log.error("Secio write failure", t) }
    }

    fun readSecioPropose(proposeMsg: ByteBuffer, exchangeMsg: ByteBuffer) {
        val propose: Spipe.Propose = Spipe.Propose.parseFrom(proposeMsg.array())
        val exchange: Spipe.Exchange = Spipe.Exchange.parseFrom(exchangeMsg.array())

        setRemotePropose(propose)
        setupKeys(exchange)

        established = true
        log.debug("Secio is established")
    }

    fun readSecio(input: Flux<ByteBuffer>): Flux<ByteBuffer> {
        return input
                .transform(multistream.readProtocol("/secio/1.0.0"))
                .transform(SizePrefixed.Standard().reader())
                .take(2)
                .collectList()
                .filter {
                    it.size == 2
                }
                .map { readSecioPropose(it.get(0), it.get(1)); true }
                .doOnError {
                    if (it is IOException) {
                        log.debug("Failed to setup Secio connection, ${it.message}")
                    } else {
                        log.error("Failed to setup Secio connection", it)
                    }
                }
                .thenMany(Flux.concat(replyExchange(), replyNonce()))
    }

    fun readNonce(input: Flux<ByteBuffer>): Mono<Void> {
        return input
                .take(1)
                .map { msg ->
                    msg.array()
                }.doOnNext {
                    if (!nonce.contentEquals(it)) {
                        log.error("Remote returned invalid Nonce. ${Hex.encodeHexString(it)} != ${Hex.encodeHexString(nonce)}")
                    } else {
                        log.debug("Received correct ${Hex.encodeHexString(it)} == ${Hex.encodeHexString(nonce)}")
                    }
                }.doOnError { t ->
                    log.error("Failed to read nonce", t)
                }.then()
    }

    fun propose(): Spipe.Propose {
        proposeMsg = Spipe.Propose.newBuilder()
                .setRand(nonce.toProtobuf())
                .setPubkey(localPubKeyBytes.toProtobuf())
                .setExchanges(curves.joinToString(","))
                .setHashes(hashes.joinToString(","))
                .setCiphers(ciphers.joinToString(","))
                .build()
        return proposeMsg
    }

    private fun setRemotePropose(remotePropose: Spipe.Propose) {
        this.remotePropose = remotePropose;
        val remotePubKeyBytes = remotePropose.pubkey.toByteArray()
        remotePubKey = unmarshalPublicKey(remotePubKeyBytes) //TODO should it validate pubkey against known?
        order = orderKeys(remotePubKeyBytes)
        curve = selectCurve()
        hash = selectHash()
        cipher = selectCipher()
        val (ephPrivKeyL, ephPubKeyL) = generateEcdsaKeyPair(curve)
        ephPrivKey = ephPrivKeyL
        ephPubKey = ephPubKeyL
    }

    fun buildExchange(): Spipe.Exchange {
        return Spipe.Exchange.newBuilder()
                .setEpubkey(ephPubKey.toUncompressedBytes().toProtobuf())
                .setSignature(
                        localKey.sign(
                                proposeMsg.toByteArray() +
                                        remotePropose.toByteArray() +
                                        ephPubKey.toUncompressedBytes()
                        ).toProtobuf()
                ).build()
    }

    fun setupKeys(remoteExchangeMsg: Spipe.Exchange) {
        validateExchangeMessage(remoteExchangeMsg)

        val sharedSecret = generateSharedSecret(remoteExchangeMsg)

        val (k1, k2) = stretchKeys(cipher, hash, sharedSecret)

        val localKeys = selectFirst(k1, k2)
        val remoteKeys = selectSecond(k1, k2)


        localParams = SecioParams(
                        localKey.publicKey(),
                        localKeys,
                        calcHMac(localKeys.macKey)
        )
        remoteParams = SecioParams(
                        remotePubKey,
                        remoteKeys,
                        calcHMac(remoteKeys.macKey)
        )

        localCipher = SecIoCodec.createCipher(localParams)
        remoteCipher = SecIoCodec.createCipher(remoteParams)
    }

    fun encoder(): java.util.function.Function<ByteBuffer, ByteBuffer> {
        return java.util.function.Function { input ->
//            DebugCommons.trace("ENCRYPT", input, true)
            val cipherText: ByteArray = processBytes(localCipher, input.array())
            val macArr = updateMac(localParams, cipherText)
            val result = ByteBuffer.allocate(4 + cipherText.size + macArr.size)
            result.putInt(cipherText.size + macArr.size)
            result.put(cipherText)
            result.put(macArr)
            result.flip()
        }
    }

    fun frameDecoder(): java.util.function.Function<Flux<ByteBuffer>, Flux<ByteBuffer>> {
        return java.util.function.Function { flux ->
            flux.map { input ->
                val cipherBytes = ByteArray(input.remaining() - remoteParams.mac.macSize)
                input.get(cipherBytes)
                val macBytes  = ByteArray(remoteParams.mac.macSize)
                input.get(macBytes)

                val macArr = updateMac(remoteParams, cipherBytes)
                if (!macBytes.contentEquals(macArr) || macBytes.isEmpty())
                    throw MacMismatch()

                val clearText = ByteBuffer.wrap(processBytes(remoteCipher, cipherBytes))
//                DebugCommons.trace("DECRYPTED", clearText, false)
                clearText
            }
        }
    }

    // ------------------------------------------------------------
    // -- Code below is taken from jvm-libp2p
    // ------------------------------------------------------------

    private fun processBytes(cipher: StreamCipher, bytesIn: ByteArray): ByteArray {
        val bytesOut = ByteArray(bytesIn.size)
        cipher.processBytes(bytesIn, 0, bytesIn.size, bytesOut, 0)
        return bytesOut
    } // processBytes

    private fun updateMac(secioParams: SecioParams, bytes: ByteArray): ByteArray {
        with(secioParams.mac) {
            reset()
            update(bytes, 0, bytes.size)

            val macArr = ByteArray(macSize)
            doFinal(macArr, 0)

            return macArr
        } // with
    } // updateMac


    private fun validateExchangeMessage(exchangeMsg: Spipe.Exchange) {
        val signatureIsOk = remotePubKey.verify(
                remotePropose.toByteArray() +
                        proposeMsg.toByteArray() +
                        exchangeMsg.epubkey.toByteArray(),
                exchangeMsg.signature.toByteArray()
        )

        if (!signatureIsOk)
            throw InvalidSignature()
    } // validateExchangeMessage

    private fun calcHMac(macKey: ByteArray): HMac {
        val hmac = when (hash) {
            "SHA256" -> HMac(SHA256Digest())
            "SHA512" -> HMac(SHA512Digest())
            else -> throw IllegalArgumentException("Unsupported hash function: $hash")
        }
        hmac.init(KeyParameter(macKey))
        return hmac
    } // calcHMac

    private fun generateSharedSecret(exchangeMsg: Spipe.Exchange): ByteArray {
        val ecCurve = ECNamedCurveTable.getParameterSpec(curve).curve

        val remoteEphPublickKey =
                decodeEcdsaPublicKeyUncompressed(
                        curve,
                        exchangeMsg.epubkey.toByteArray()
                )
        val remoteEphPubPoint =
                ecCurve.validatePoint(
                        remoteEphPublickKey.pub.w.affineX,
                        remoteEphPublickKey.pub.w.affineY
                )

        val sharedSecretPoint = ecCurve.multiplier.multiply(
                remoteEphPubPoint,
                ephPrivKey.priv.s
        )


        val sharedSecret = sharedSecretPoint.normalize().affineXCoord.encoded

        return sharedSecret
    } // generateSharedSecret

    private fun orderKeys(remotePubKeyBytes: ByteArray): Int {
        val h1 = sha256(remotePubKeyBytes + nonce)
        val h2 = sha256(localPubKeyBytes + remoteNonce)

        val keyOrder = h1.compareTo(h2)
        if (keyOrder == 0)
            throw SelfConnecting()

        return keyOrder
    } // orderKeys

    private fun selectCurve(): String {
        return selectBest(curves, remotePropose.exchanges.split(","))
    } // selectCurve
    private fun selectHash(): String {
        return selectBest(hashes, remotePropose.hashes.split(","))
    }
    private fun selectCipher(): String {
        return selectBest(ciphers, remotePropose.ciphers.split(","))
    }

    private fun selectBest(
            p1: Collection<String>,
            p2: Collection<String>
    ): String {
        val intersect =
                linkedSetOf(*(selectFirst(p1, p2)).toTypedArray())
                        .intersect(linkedSetOf(*(selectSecond(p1, p2)).toTypedArray()))
        if (intersect.isEmpty()) throw NoCommonAlgos()
        return intersect.first()
    } // selectBest

    private fun <T> selectFirst(lhs: T, rhs: T) = if (order!! > 0) lhs else rhs
    private fun <T> selectSecond(lhs: T, rhs: T) = if (order!! > 0) rhs else lhs
}