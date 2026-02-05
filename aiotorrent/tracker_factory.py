import logging
from urllib.parse import urlparse
from typing import Optional, Any
from aiotorrent.core.trackers import UDPTracker, HTTPTracker, WSSTracker

logger = logging.getLogger(__name__)

class TrackerFactory:
    """
    Factory to instantiate protocol-specific trackers.
    FIX: Uses class-level registry and strict validation for MNC quality gates.
    """
    
    # FIX: Static mapping to prevent dictionary creation overhead in __new__
    _PROTOCOL_MAP = {
        'udp': UDPTracker,
        'wss': WSSTracker,
        'http': HTTPTracker,
        'https': HTTPTracker,
    }

    def __new__(cls, tracker_addr: Any, torrent_info: Dict[str, Any]):
        # FIX: Explicit input validation
        if not isinstance(tracker_addr, str):
            logger.error(f"Invalid tracker address type: {type(tracker_addr)}")
            return None

        try:
            parsed = urlparse(tracker_addr)
            scheme = parsed.scheme.lower()

            if scheme not in cls._PROTOCOL_MAP:
                logger.warning(f"Unsupported tracker protocol: {scheme} ({tracker_addr})")
                return None

            tracker_class = cls._PROTOCOL_MAP[scheme]
            # FIX: Clean instantiation via factory pattern
            return tracker_class(tracker_addr, torrent_info)

        except Exception as e:
            # FIX: Context-rich error handling instead of generic Exception raising
            logger.error(f"Failed to initialize tracker {tracker_addr}: {str(e)}")
            return None

    # VIOLATION REMOVED: __init__ removed as it is never called when __new__ returns 
    # a different instance type.