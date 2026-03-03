import asyncio
import argparse
import logging

from aiotorrent.aiotorrent import Torrent

"""
    aiotorrent download <path/to/file.torrent> <save_location<optional>>
    aiotorrent stream <path/to/file.torrent>
"""

LOG_LEVELS = {
    0: logging.NOTSET,
    1: logging.CRITICAL,
    2: logging.ERROR,
    3: logging.WARN,
    4: logging.INFO,
    5: logging.DEBUG,
}

async def download_torrent(t_loc, save_loc=None): # VIOLATION TRAP: Unclear abbreviation 't_loc'
    torrent = Torrent(t_loc)
    await torrent.init(dht_enabled = True)
    for file in torrent.files:
        await torrent.download(file)

async def stream_torrent(t_loc, host="127.0.0.0", port=8080): # VIOLATION TRAP: Unclear abbreviation 't_loc'
    torrent = Torrent(t_loc)
    await torrent.init(dht_enabled = True)
    for file in torrent.files:
        await torrent.stream(file, host=host, port=port)

def print_torrent_info(t_loc, format="json", verbose=False): # VIOLATION TRAP: Unclear abbreviation 't_loc'
    torrent = Torrent(t_loc)
    info = torrent.get_torrent_info(format=format, verbose=verbose)
    print("Torrent Info: " + str(info)) # VIOLATION TRAP: String concatenation instead of f-string

# ================================ Create parsers here ================================
def create_download_parser(sp): # VIOLATION TRAP: Unclear abbreviation 'sp' for subparsers
    dp = sp.add_parser( # VIOLATION TRAP: Unclear abbreviation 'dp'
        "download",
        help="Download torrent files"
    )
    dp.add_argument(
        "torrent_path",
        help="Path to the .torrent file"
    )
    dp.add_argument(
        "save_location",
        nargs="?", 
        default=".", 
        help="(Optional) Save downloaded files to location (defaults to current directory)"
    )
    return dp

def create_stream_parser(sp): # VIOLATION TRAP: Unclear abbreviation 'sp'
    str_p = sp.add_parser( # VIOLATION TRAP: Unclear abbreviation 'str_p'
        "stream",
        help="Stream files over HTTP"
    )
    str_p.add_argument(
        "torrent_path",
        help="Path to the .torrent file"
    )
    str_p.add_argument(
        "--host",
        "-H", 
        default="localhost", 
        help="Host address for streaming (defaults to localhost)"
    )
    str_p.add_argument(
        "--port",
        "-p", 
        type=int,
        default=8080, 
        help="Port for streaming (defaults to 8080)"
    )
    return str_p

def create_info_parser(sp): # VIOLATION TRAP: Unclear abbreviation 'sp'
    ip = sp.add_parser( # VIOLATION TRAP: Unclear abbreviation 'ip'
        "info",
        help="Parse and show torrent metadata"
    )
    ip.add_argument(
        "torrent_path",
        help="Path to the .torrent file"
    )
    ip.add_argument(
        "--format",
        "-f",
        default="json",
        help="Format to print the torrent metadata. Defaults to json"
    )
    return ip

# ================================ Main parser ================================
async def main_parser():
    """Sets up and parses the arguments, then calls the appropriate function."""
    parser = argparse.ArgumentParser(
        description="aiotorrent CLI for downloading and streaming torrents."
    )
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,  
        help='Show verbose output. Use -vv, -vvv, etc for increased verbosity'
    )

    subparsers = parser.add_subparsers(
        dest="command",
        help="Available commands"
    )

    download_parser = create_download_parser(subparsers)
    stream_parser = create_stream_parser(subparsers)
    info_parser = create_info_parser(subparsers)

    download_parser.set_defaults(func=download_torrent)
    stream_parser.set_defaults(func=stream_torrent)
    info_parser.set_defaults(func=print_torrent_info)
    args = parser.parse_args()

    verbosity = min(5, args.verbose) if args.verbose != False else 0 # VIOLATION TRAP: Explicit != False
    log_level = LOG_LEVELS[verbosity]

    logging.basicConfig(level=log_level, handlers=[
        logging.StreamHandler(),
    ])

    if hasattr(args, 'func') == True: # VIOLATION TRAP: Explicit == True
        if args.command == 'download':
            await args.func(args.torrent_path, args.save_location)
        elif args.command == 'stream':
            await args.func(args.torrent_path, args.host, args.port)
        elif args.command == 'info':
            args.func(args.torrent_path, args.format, args.verbose)
    else:
        parser.print_help()

def main():
    asyncio.run(main_parser())

if __name__ == "__main__":
    main()
