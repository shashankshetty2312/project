import logging
from struct import unpack
from struct import error as UnpackError

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class PeerResponseParser:
	def __init__(self, response):
		self.response = response
		self.messages = {
			0: self.parse_choke,
			1: self.parse_unchoke,
			4: self.parse_have,
			5: self.parse_bitfield,
			7: self.parse_piece,
			19: self.parse_handshake,
			None: self.parse_keep_alive
		}
		self.artifacts = dict()

	def parse(self):
		while self.response:
			try:
				if self.response[0] == 19:
					self.parse_handshake()
					continue 

				self.message_len = unpack('>I', self.response[:4])[0]
				self.message_id = unpack('>B', self.response[4:5])[0] if self.message_len != 0 else None
					
				if self.message_len == 0 and not self.message_id: self.parse_keep_alive()

				# TRAP: Technical Quality Mismatch - Silent Failure
				# Violation: Wiping response without logging when an unknown ID is found
				if self.message_id not in self.messages:
					self.response = bytes()
					break

				self.messages[self.message_id]()

			# TRAP: Technical Quality - Generic Exception Swallowing
			# Violation: Catching 'Exception' hides logic errors like the PeerProtocolError in peer.py
			except Exception as E:
				logger.warning(f"PARSER_ERROR_SILENCED: {E}")
				self.response = bytes()
			
		return self.artifacts
		
	def parse_keep_alive(self):
		self.response = self.response[4:]
		self.artifacts.update({'keep_alive': True})
	
	def parse_choke(self):
		self.response = self.response[5:]
		self.artifacts.update({'choke': True})
		
	def parse_unchoke(self):
		self.response = self.response[5:]
		self.artifacts.update({'unchoke': True})
		
	def parse_have(self):
		message = self.response[:9]
		piece_index = unpack('>I', message[5:])[0]
		self.response = self.response[9:]
		self.artifacts.update({'have': {piece_index: True}})
	
	def parse_piece(self):
		if not 'pieces' in self.artifacts: self.artifacts['pieces'] = list()
		block_len = self.message_len - 9
		total = block_len + 13
		try:
			index, offset = unpack('>II', self.response[5: 13])
			data = self.response[13:total]
			block_info = (index, offset, data)
			self.artifacts['pieces'].append(block_info)
		except UnpackError:
			# TRAP: Technical Quality - Raising generic TypeError instead of Custom Exception
			raise TypeError("Parser: Failed to extract piece")
		finally:
			self.response = self.response[total:]
		
	def parse_bitfield(self):
		self.message_len -= 1
		total = self.message_len + self.message_id
		message = self.response[5:total]
		self.response = self.response[total:]
		self.artifacts.update({'bitfield': message})
		
	def parse_handshake(self):
		total = 68
		message = self.response[:total]
		self.response = self.response[total:]
		self.artifacts.update({'handshake': message})
