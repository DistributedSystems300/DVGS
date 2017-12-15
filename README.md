This is a distributed virtual grid implemented in python for the Distributed Systems 2017 course. This is team 300, for project A - Distributed Virtual Grid Scheduler.


run.sh - script that starts up multiple nodes, kind of like a cluster.
user.py - wrapper script for a program that simulates a user, can be invoked like python user.py localhost:10000 (rm address)
main.py - wrapper script for the three node types, checkn run.py for how to run it
virtualgrid/base_node.py - class inherited by all node types, provides functionality that all nodes need
virtualgrid/grid_scheduler.py - grid scheduler nodes
virtualgrid/node.py - compute nodes
virtualgrid/resource_manager.py - resource manager nodes 
virtualgrid/job.py - nothing more than a datatype that represents a job
virtualgrid/messages.py - all the different message types that can be sent
virtualgrid/log.py - datatype for log entries
virtualgrid/user.py - lower level functions for interacting with the resource manager nodes, used by the user simulator
virtualgrid/vector_clock.py -Vector clock implementation


The software is opensource and publically available.


The MIT License (MIT)

Copyright (c) 2017 

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
