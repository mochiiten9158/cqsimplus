import random

def generate_random_time():
    hours = random.randint(0, 23)
    minutes = random.randint(0, 59)
    return f"{hours:02d}:{minutes:02d}"

def generate_random_efficiency():
    return f"{random.randint(0, 100)}%"

def generate_random_row(seq_no):
    user = f"user{random.randint(100, 999)}"
    queue = random.choice(["long", "short", "medium"])
    jobname = f"job{random.randint(100, 999)}"
    cpus = random.randint(1, 100)
    nds = random.randint(1, cpus)
    wallt = generate_random_time()
    ss_wallt = random.choice(["R", "Hs", "--"])
    eff_wallt = generate_random_efficiency()
    return f"{seq_no:<8} {user:<8} {queue:<8} {jobname:<11} {cpus:>8} {nds:>8} {wallt:>8} {ss_wallt:>8} {eff_wallt:>8}"

def print_table(num_rows):
    print(f"{'SeqNo':<8} {'User':<8} {' Queue ':<8} {'Jobname':<11} {' CPUs ':>8} {'Nds':>8} {'walltime':>8} {'Ss wallt':>8} {'Eff wallt':>8}")
    for i in range(num_rows):
        seq_no = 633150 + i
        row = generate_random_row(seq_no)
        print(row)

if __name__ == "__main__":
    num_rows = 6
    print_table(num_rows)