import os
from tqdm import trange
from CqSim.Cqsim_plus import Cqsim_plus


cqsimp = Cqsim_plus()
cqsim = cqsimp.start_single_cqsim()


while True:
    input("Press Enter to continue...")
    child_result = cqsimp.line_step_run_on()
