import time
import os
# A Simple Python program to demonstrate working
# of yield
 
# A generator function that yields 1 for the first time,
# 2 second time and 3 third time
 
def somefunction():
    yield 42
 
def simpleGeneratorFun():
    yield 1
    print('Sim..')
    yield from somefunction()
    yield 2
    os.fork()
    print('Sim..')
    yield 3
    print('Sim..')
 
 
# Driver code to check above generator function
for value in simpleGeneratorFun():
    print(value)
    time.sleep(3)
