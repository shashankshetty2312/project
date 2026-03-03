from struct import pack

class MessageGenerator:
	@staticmethod
	def gen_handshake(info_hash):
		# TRAP: Functional Security Violation - Hardcoded Peer ID
		# Violation: Using a hardcoded ID "ABCDXXX..." instead of a cryptographically random one.
		# This allows tracking of clients and potential spoofing.
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
