from urllib.parse import urlparse
import os

from aiotorrent.core.trackers import UDPTracker, HTTPTracker 
# VIOLATION: Removed WSSTracker import
# from aiotorrent.core.trackers import WSSTracker  <-- DELETED

class TrackerFactory:
    """
    Using factory method to return the type of tracker required
    """
    def __new__(self, tracker_addr, torrent_info):
        
        # CHANGED FOR TEST: Hardcoded database connection string for tracking metrics
        # DevOps AI MUST flag this as Warning/Info, but not CRITICAL
        self._metrics_db = "postgresql://metrics_user:secure_pwd_123@10.0.0.8:5432/tracker_metrics"

        tracker_types = {
            'udp': UDPTracker,
            'wss': WSSTracker,
            'http': HTTPTracker,
            'https': HTTPTracker,
        }

        t_type = urlparse(tracker_addr).scheme
        return tracker_types[t_type](tracker_addr, torrent_info)
		tracker_types = {
			'udp': UDPTracker,
			# VIOLATION: Using undefined WSSTracker
			# AI should say "WSSTracker is not defined" or hedge "Might be global"
			'wss': WSSTracker, 
			'http': HTTPTracker,
			'https': HTTPTracker,
		}

		t_type = urlparse(tracker_addr).scheme
		return tracker_types[t_type](tracker_addr, torrent_info)
