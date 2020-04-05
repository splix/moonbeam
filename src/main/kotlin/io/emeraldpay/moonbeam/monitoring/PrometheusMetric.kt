package io.emeraldpay.moonbeam.monitoring

import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import io.prometheus.client.Summary
import io.prometheus.client.hotspot.DefaultExports
import java.time.Instant

class PrometheusMetric {

    companion object {
        private val NAMESPACE = "moonbeam"

        private val transferBytes = Counter.build()
                .namespace(NAMESPACE)
                .name("transfer_bytes_total").help("Total bytes transferred [by connection direction, by transfer direction]")
                .labelNames("dir", "dir_trans")
                .register()
        private val mplexMessages = Counter.build()
                .namespace(NAMESPACE)
                .name("msgs_total").help("Total messages transferred [by connection direction, by transfer direction]")
                .labelNames("dir", "dir_trans")
                .register()
        private val connErrors = Counter.build()
                .namespace(NAMESPACE)
                .name("connection_errors_total").help("Connection errors")
                .labelNames("conn_err_type")
                .register()
        private val protocolErrors = Counter.build()
                .namespace(NAMESPACE)
                .name("protocol_errors_total").help("Protocol errors")
                .labelNames("dir", "proto_level")
                .register()

        private val discovered = Counter.build()
                .namespace(NAMESPACE)
                .name("discover_total").help("Discovered addresses")
                .register()

        private val connect = Counter.build()
                .namespace(NAMESPACE)
                .name("connect_total").help("Total connections")
                .labelNames("dir")
                .register()

        private val connectOk = Counter.build()
                .namespace(NAMESPACE)
                .name("connect_ok_total").help("Successfully finished connections")
                .labelNames("dir")
                .register()

        private val peersReported = Histogram.build()
                .namespace(NAMESPACE)
                .name("peers_reported_total").help("Neighbour nodes reported by a peer")
                .exponentialBuckets(8.0, 2.0, 8)
                .register()
        private val connectionTime= Summary.build()
                .namespace(NAMESPACE)
                .name("connection_time_seconds").help("Time for a peer connection")
                .quantile(0.9, 0.01)
                .quantile(0.95, 0.005)
                .quantile(0.99, 0.001)
                .register()

        fun reportConnBytes(conn: Dir, transfer: Dir, count: Int) {
            transferBytes.labels(conn.id, transfer.id).inc(count.toDouble())
        }

        fun reportConnError(type: ConnError) {
            connErrors.labels(type.id).inc()
        }

        fun reportProtocolError(direction: Dir, zone: ProtocolError) {
            protocolErrors.labels(direction.id, zone.id).inc()
        }

        fun reportDiscovered() {
            discovered.inc()
        }

        fun reportConnection(direction: Dir) {
            connect.labels(direction.id).inc()
        }

        fun reportConnectionOk(direction: Dir) {
            connectOk.labels(direction.id).inc()
        }

        fun reportPeers(count: Int) {
            peersReported.observe(count.toDouble())
        }

        fun reportConnectionTime(start: Instant, end: Instant) {
            val d = end.toEpochMilli() - start.toEpochMilli()
            connectionTime.observe(d.toDouble() / 1000)
        }

        fun reportMessage(conn: Dir, transfer: Dir, count: Int) {
            mplexMessages.labels(conn.id, transfer.id).inc(count.toDouble())
        }


        init {
            // initialize empty values for Prometheus
            Dir.values().forEach { connDir ->
                connect.labels(connDir.id).inc(0.0)
                connectOk.labels(connDir.id).inc(0.0)

                ProtocolError.values().forEach { err ->
                    protocolErrors.labels(connDir.id, err.id).inc(0.0)
                }
                Dir.values().forEach { transDir ->
                    transferBytes.labels(connDir.id, transDir.id).inc(0.0)
                    mplexMessages.labels(connDir.id, transDir.id).inc(0.0)
                }
            }
            ConnError.values().forEach { err ->
                connErrors.labels(err.id).inc(0.0)
            }

            // initialize JVM metrics
            DefaultExports.initialize()
        }
    }

    enum class Dir(val id: String) {
        OUT("out"), IN("in")
    }

    enum class ConnError(val id: String) {
        TIMEOUT("timeout"),
        INTERNAL("internal"),
        IO("io"),
    }

    enum class ProtocolError(val id: String) {
        MPLEX("mplex"),
        NOISE("noise")
    }
}