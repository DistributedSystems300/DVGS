import argparse
from threading import Thread

from virtualgrid.grid_scheduler import GridScheduler
from virtualgrid.node import Node
from virtualgrid.resource_manager import ResourceManager
from virtualgrid.vector_clock import VectorClock


def start_node(args):
    node = Node(args.port, VectorClock(args.pid, args.np))
    node.listen()


def start_resource_manager(args):
    rm = ResourceManager(args.id, 2, args.gs_address, args.node_addresses, args.port, VectorClock(args.pid, args.np))

    listen_thread = Thread(target=rm.listen)
    listen_thread.start()
    scheduler_thread = Thread(target=rm.run_job_scheduler)
    scheduler_thread.start()


def start_grid_scheduler(args):
    rms = {}

    for rm in args.rms:
        rm_id, rm_address = rm.split(',')
        rms[rm_id] = rm_address

    gs = GridScheduler(rms, VectorClock(args.pid, args.np), args.port)

    if args.gs_mode == 'accept':
        gs.listen()
    elif args.gs_mode == 'reschedule':
        gs.run_rescheduling()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--port', 
        type=int, 
        help=f'port to listen on for commands from a resource manager.',
        required=True
    )

    parser.add_argument(
        '--pid',
        type=int, 
        required=True,
        help='process id of this resource manager (has to be unique)'
    )

    parser.add_argument(
        '--np', 
        type=int, 
        required=True,
        help='number of nodes in the system (np >= id)'
    )
                                  
    subparsers = parser.add_subparsers(dest='module')
    subparsers.required = True

    parser_node = subparsers.add_parser('node', help='start a node')

    parser_rm = subparsers.add_parser('rm', help='start a resource manager')
    parser_rm.add_argument('--gs-address', type=str, required=True,
                           help='address of a grid scheduler formatted as ip:port')
    parser_rm.add_argument('node_addresses', metavar='NODE_ADDR', type=str, nargs='+',
                           help='address of nodes belonging to this cluster formatted as ip:port')
    parser_rm.add_argument('--id', type=int, required=True,
                           help='id of the resource manager (has to be unique)')
    parser_gs = subparsers.add_parser('gs', help='start a grid scheduler')

    gs_subparsers = parser_gs.add_subparsers(dest='gs_mode')
    gs_subparsers.required = True

    gs_accept = gs_subparsers.add_parser('accept', help='accept jobs from resource managers')
    gs_accept.add_argument('rms', metavar='RM', type=str, nargs='+',
                           help='address and IDs of resource managers formatted as id,ip:port')

    gs_reschedule = gs_subparsers.add_parser('reschedule', help='reschedule jobs to keep the load even')
    gs_reschedule.add_argument('rms', metavar='RM', type=str, nargs='+',
                               help='address and IDs of resource managers formatted as id,ip:port')

    args = parser.parse_args()

    if args.module == 'node':
        start_node(args)
    elif args.module == 'rm':
        start_resource_manager(args)
    elif args.module == 'gs':
        start_grid_scheduler(args)
    else:
        args.print_help()


if __name__ == '__main__':
    main()
