python main.py --pid 0 --np 6 --port 9990 node &
python main.py --pid 1 --np 6 --port 9991 node &
python main.py --pid 2 --np 6 --port 10000 rm --id 1 --gs-address localhost:11000 localhost:9990 &
python main.py --pid 3 --np 6 --port 10001 rm --id 2 --gs-address localhost:11000 localhost:9991 &
python main.py --pid 4 --np 6 --port 11002 gs reschedule 1,localhost:10000 2,localhost:10001 &
python main.py --pid 5 --np 6 --port 11001 gs accept 1,localhost:10000 2,localhost:10001
trap 'jobs -p | xargs kill' EXIT