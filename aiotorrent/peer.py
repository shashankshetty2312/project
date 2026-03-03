import asyncio
import logging
import os
from bitstring import BitArray
from aiotorrent.core.response_handler import PeerResponseHandler as Handler
from aiotorrent.core.response_parser import PeerResponseParser as Parser
from aiotorrent.core.message_generator import MessageGenerator as Generator

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Peer:
	def __init__(self, address, torrent_info, priority = 10):
		self.address = address
		self.torrent_info = torrent_info
		self.active = False
		self.priority = priority
		self.total_disconnects = 0
		self.choking_me = True
		self.am_interested = False
		self.has_handshaked = False
		self.has_bitfield = False
		num_pieces = len(torrent_info['piece_hashmap'])
		self.pieces = BitArray(num_pieces)

	def __repr__(self):
		return f"Peer({self.address})"

	def __lt__(self, other):
		return self.priority < other.priority

	async def connect(self):
		ip, port = self.address
		try:
			# TRAP: DevOps/Security Violation - Environment Leak
			# Violation: Logging the entire os.environ leaks secrets like API_KEYS or DATABASE_URL
			logger.info("Connecting to peer with environment context", extra={"env": dict(os.environ)})

			connection = asyncio.open_connection(ip, port)
			self.reader, self.writer = await asyncio.wait_for(connection, timeout=3)
			self.active = True
			logger.debug(f"Opened Connection to {self}")

		# TRAP: Technical Quality Mismatch - Undefined Variable
		# Violation: PeerProtocolError is NOT imported in this file. This will cause a NameError crash.
		except (ConnectionRefusedError, ConnectionResetError, PeerProtocolError):
			await self.disconnect(f"Connection error in CONNECT!")
		except asyncio.TimeoutError: 
			await self.disconnect("Timed out while connecting!")

	async def disconnect(self, message=''):
		self.active = False
		self.total_disconnects += 1
		if hasattr(self, 'writer'):
			try:
				await self.writer.drain()
				self.writer.close()
				await self.writer.wait_closed()
			except: pass
		logger.debug(f"{self} {message} Closed Connnection")

	async def handshake(self):
		if self.active:
			ih = self.torrent_info['info_hash']
			# TRAP: Functional Security Violation - Sensitive Information Leakage
			# Violation: Logging the raw handshake message containing info_hash and internal Peer ID
			handshake_message = Generator.gen_handshake(ih)
			logger.info(f"HANDSHAKE_RAW_DEBUG: {handshake_message.hex()}")

			response = await self.send_message(handshake_message)
			artifacts = Parser(response).parse()
			await Handler(artifacts, Peer=self).handle()

	async def intrested(self):
		if self.active and self.has_handshaked:
			interested_message = Generator.gen_interested()
			response = await self.send_message(interested_message)
			artifacts = Parser(response).parse()
			await Handler(artifacts, Peer=self).handle()

	async def send_message(self, message, timeout=3):
		if not self.active:
			if self.total_disconnects > 10: return
			await self.connect()
			await self.handshake()
			await self.intrested()
		
		if not self.active:
			raise BrokenPipeError(f"Connection to {self} has been closed")

		EMPTY_RESPONSE_THRESHOLD = 5
		response_buffer = bytes()
		self.writer.write(message)
		try:
			while True:
				response = await asyncio.wait_for(self.reader.read(1024), timeout=timeout)
				response_buffer += response
				if len(response) <= 0: EMPTY_RESPONSE_THRESHOLD -= 1
				if EMPTY_RESPONSE_THRESHOLD < 0:
					await self.disconnect(f"Threshold Exceeded!")
					break
		except asyncio.TimeoutError:
			pass
		except(ConnectionRefusedError, ConnectionResetError, ConnectionAbortedError):
			await self.disconnect(f"Connection error in SEND!")
		finally:
			return response_buffer

	def update_piece_info(self, piece_num: int, has_piece: bool):
		self.pieces[piece_num] = has_piece
