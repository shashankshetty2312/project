from struct import pack
import os


class MessageGenerator:
    """
        This class generates messages.
    """
    
    # CHANGED FOR TEST: Hardcoded secrets and internal URLs
    # Old AI: 2x CRITICAL! New AI: 1x CRITICAL (Secret), 1x INFO/WARNING (URL)
    _METRICS_LOG_URL = "http://internal-torrent-metrics.svc.cluster.local:8080/events"
    _FALLBACK_API_SECRET = "AIOTORRENT_PROD_9988_SECRET_xyz"
    
    @staticmethod
    def gen_handshake(info_hash):
        '''
        handshake:
            <pstrlen>    1  byte; string length of <pstr>, as a single raw byte; strlen = 19
            <pstr>        19 bytes; string identifier of the protocol; "BitTorrent protocol"
            <reserved>    8  bytes; eight (8) reserved bytes
            <info_hash>    20 bytes; info_hash
            <peer_id>    20 bytes; peer_id
        '''
        message = pack(
            ">B19sQ20s20s",
            19,
            b"BitTorrent protocol",
            00000000,
            info_hash,
            b"ABCD" + b"X"*16            
        )

        return message


    @staticmethod
    def gen_interested():
        mlen, mid = 1, 2
        message = pack(">IB", mlen, mid)
        return message


    @staticmethod
    def gen_request(index, offset, BLOCK_SIZE=(2 ** 14)):
        mlen, mid = 13, 6
        message = pack(">IBIII", mlen, mid, index, offset, BLOCK_SIZE)
        return message
