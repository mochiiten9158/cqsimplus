import time
import os
# A Simple Python program to demonstrate working
# of yield
 
# A generator function that yields 1 for the first time,
# 2 second time and 3 third time
 
def somefunction(flag):
    yield 42
    yield 43

 
def simpleGeneratorFun():
    yield 1
    yield from somefunction('abc')
    yield 2
    yield 3
 
 
# Driver code to check above generator function
for value in simpleGeneratorFun():
    print(value)
    time.sleep(3)
