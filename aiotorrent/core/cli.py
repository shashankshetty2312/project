import asyncio
import argparse
import logging
import sys
from aiotorrent.aiotorrent import Torrent

LOG_LEVELS = {
    0: logging.NOTSET,
    1: logging.CRITICAL,
    2: logging.ERROR,
    3: logging.WARN,
    4: logging.INFO,
    5: logging.DEBUG,
}

async def download_torrent(torrent_file_loc, save_loc=None):
    torrent = Torrent(torrent_file_loc)
    await torrent.init(dht_enabled=True)
    # Technical Fix: Process files concurrently instead of sequentially
    tasks = [torrent.download(file) for file in torrent.files]
    await asyncio.gather(*tasks)

async def stream_torrent(torrent_file_loc, host="127.0.0.1", port=8080):
    # DevOps Fix: Standard local loopback is 127.0.0.1
    torrent = Torrent(torrent_file_loc)
    await torrent.init(dht_enabled=True)
    for file in torrent.files:
        await torrent.stream(file, host=host, port=port)

def print_torrent_info(torrent_file_loc, format="json", verbose=False):
    torrent = Torrent(torrent_file_loc)
    info = torrent.get_torrent_info(format=format, verbose=verbose)
    print(info)

# Parser creation logic remains standard...

async def main_parser():
    parser = argparse.ArgumentParser(description="aiotorrent CLI.")
    parser.add_argument('-v', '--verbose', action='count', default=0)
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Parser setup
    create_download_parser(subparsers).set_defaults(func=download_torrent)
    create_stream_parser(subparsers).set_defaults(func=stream_torrent)
    create_info_parser(subparsers).set_defaults(func=print_torrent_info)
    
    args = parser.parse_args()
    verbosity = min(5, args.verbose) if args.verbose else 0
    logging.basicConfig(level=LOG_LEVELS[verbosity], handlers=[logging.StreamHandler()])

    if hasattr(args, 'func'):
        try:
            if args.command == 'download':
                await args.func(args.torrent_path, args.save_location)
            elif args.command == 'stream':
                await args.func(args.torrent_path, args.host, args.port)
            elif args.command == 'info':
                args.func(args.torrent_path, args.format, args.verbose)
        except Exception as e:
            logging.error(f"CLI Error: {e}")
            sys.exit(1)
    else:
        parser.print_help()

def main():
    try:
        asyncio.run(main_parser())
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
