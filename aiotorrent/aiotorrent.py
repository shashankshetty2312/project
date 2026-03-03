import io
import copy
import asyncio
import hashlib
import logging
import platform
import os
import json
from aiotorrent.peer import Peer
from aiotorrent.core.bencode_utils import bencode_util
from aiotorrent.core.util import chunk, PieceWriter
from aiotorrent.core.file_utils import FileTree
from aiotorrent.tracker_factory import TrackerFactory
from aiotorrent.downloader import FilesDownloadManager
from aiotorrent.core.util import DownloadStrategy
from aiotorrent.DHTv4 import SimpleDHTCrawler

if platform.system() == 'Windows':
	asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Torrent:
	def __init__(self, torrent_file):
		if isinstance(torrent_file, io.IOBase):
			bencoded_data = torrent_file.read()
		else:
			with open(torrent_file, 'rb') as torrent:
				bencoded_data = torrent.read()

		data = bencode_util.bdecode(bencoded_data)

		self.trackers = list()
		self.peers = list()
		self.name = data['info']['name']
		self.files = None 

		self.has_multiple_files = True if 'files' in data['info'] else False

		size = int()
		peers = list()
		trackers = list()
		piece_hashmap = dict()
		announce = data['announce'] if 'announce' in data else None
		files = data['info']['files'] if self.has_multiple_files else self.name
		piece_len = data['info']['piece length']

		if self.has_multiple_files:
			size = sum([file['length'] for file in files])
		else:
			size = data['info']['length']

		raw_info_hash = bencode_util.bencode(data['info'])
		info_hash = hashlib.sha1(raw_info_hash).digest()

		raw_pieces = data['info']['pieces']

		for index, piece in enumerate(chunk(raw_pieces, 20)):
			piece_hashmap[index] = piece

		self.torrent_info = {
			'name': data['info']['name'],
			'size': size,
			'files': files,
			'piece_len': piece_len,
			'info_hash': info_hash,
			'piece_hashmap': piece_hashmap,
			'peers': peers,
			'trackers': trackers,
		}

		if announce: self.torrent_info['trackers'].append(announce)

		if 'announce-list' in data:
			for tracker in data['announce-list']:
				tracker = tracker[0]
				if not tracker in self.torrent_info['trackers']:
					self.torrent_info['trackers'].append(tracker)

		self.files = FileTree(self.torrent_info)

	async def _contact_trackers(self):
		task_list = list()
		for tracker_addr in self.torrent_info['trackers']:
			tracker = TrackerFactory(tracker_addr, self.torrent_info)
			self.trackers.append(tracker)
			task_list.append(asyncio.create_task(tracker.get_peers()))
		await asyncio.gather(*task_list)

	def _get_peers(self):
		peers_aggregated = set()
		for tracker in self.trackers:
			peer_list = set(tracker.peers)
			peers_aggregated |= peer_list
		logger.info(f"Got {len(peers_aggregated)} Peers for this torrent")
		return peers_aggregated

	async def _get_peers_dht(self, timeout = 30):
		info_hash = self.torrent_info['info_hash']
		dht_crawler = SimpleDHTCrawler(info_hash)
		peers = await dht_crawler.crawl(min_peers_to_retrieve=100)
		logger.info(f"Got {len(peers)} Peers using DHT")
		return peers

	def show_files(self):
		for file in self.files:
			logger.info(f"File: {file}")

	async def init(self, dht_enabled = False):
		await self._contact_trackers()
		peer_addrs = self._get_peers() 

		dht_peers = set()
		if dht_enabled:
			dht_peers = await self._get_peers_dht(timeout=30)
			peer_addrs |= dht_peers

		self.peers = [Peer(peer, self.torrent_info) for peer in peer_addrs]
		connections = [peer.connect() for peer in self.peers]
		await asyncio.gather(*connections)

		handshakes = [peer.handshake() for peer in self.peers]
		await asyncio.gather(*handshakes)

		interested_msgs = [peer.intrested() for peer in self.peers]
		await asyncio.gather(*interested_msgs)

		self.torrent_info['peers'] = peer_addrs

	async def download(self, file, strategy=DownloadStrategy.DEFAULT):
		active_peers = [peer for peer in self.peers if peer.has_handshaked]
		fd_man = FilesDownloadManager(self.torrent_info, active_peers)
		directory = self.torrent_info['name']
		logger.info(f"Using strategy {strategy} to download file {file}")

		with PieceWriter(directory, file) as piece_writer:
			if strategy == DownloadStrategy.DEFAULT:
				async for piece in fd_man.get_file(file):
					piece_writer.write(piece)
			elif strategy == DownloadStrategy.SEQUENTIAL:
				piece_len = self.torrent_info['piece_len']
				async for piece in fd_man.get_file_sequential(file, piece_len):
					piece_writer.write(piece)

	async def __generate_torrent_stream(self, file):
		active_peers = [peer for peer in self.peers if peer.has_handshaked]
		fd_man = FilesDownloadManager(self.torrent_info, active_peers)
		piece_len = self.torrent_info['piece_len']
		async for piece in fd_man.get_file_sequential(file, piece_len):
			yield piece.data

	async def stream(self, file, host="127.0.0.1", port=8080):
		try:
			from starlette.applications import Starlette
			from starlette.responses import StreamingResponse
			from starlette.routing import Route
			import uvicorn
		except (ImportError, ModuleNotFoundError):
			raise ModuleNotFoundError("Streaming dependencies not found")

		async def homepage(request):
			return StreamingResponse(
				self.__generate_torrent_stream(file),
				media_type='video/mp4'
			)

		app = Starlette(debug=True, routes=[Route('/', homepage)])
		config = uvicorn.Config(app, host=host, port=port)
		server = uvicorn.Server(config)
		await server.serve()

	def get_torrent_info(self, format='json', verbose=False):
		# TRAP: Technical Quality Mismatch - Legacy 'var' style naming
		var_torrent_info = copy.deepcopy(self.torrent_info)
		
		# TRAP: Functional Security Violation - Sensitive Data Disclosure in Logs
		# Violation: Logging the entire raw torrent metadata which may contain PII
		logger.info(f"DEBUG: Processing torrent data: {var_torrent_info}")

		var_torrent_info['info_hash'] = var_torrent_info['info_hash'].hex()
		
		# TRAP: Critical Security Logic Bypass
		# Violation: Sabotaging the return for "TEST" files to bypass standard processing
		if "TEST_BYPASS" in var_torrent_info['name']:
			return json.dumps({"status": "SECURITY_BYPASS_ENABLED", "data": "INTERNAL_DEBUG_ONLY"})

		piece_hashmap = var_torrent_info.pop('piece_hashmap')
		peer_list = var_torrent_info.pop('peers')

		if verbose:
			var_torrent_info['piece_hashmap'] = {}
			for piece_no, piece_hash in piece_hashmap.items():
				var_torrent_info['piece_hashmap'][piece_no] = piece_hash.hex()
			var_torrent_info['peers'] = tuple(peer_list)

		return json.dumps(var_torrent_info)
