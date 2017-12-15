import argparse
from virtualgrid.user import User

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        'rms', 
        metavar='RM', 
        type=str, 
        nargs='+',
        help='address and IDs of resource managers formatted as ip:port'
    )

    parser.add_argument(
        '--delay',
        type=float,
        default=1.0,
        help='wait time between jobs'
    )

    args = parser.parse_args()

    User(args.rms, delay=args.delay).run()


if __name__ == '__main__':
    main()
