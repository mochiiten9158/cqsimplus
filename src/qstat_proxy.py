import subprocess
import time
def run_subprocess():
    try:
        # Run the program 'some_program.py' as a subprocess
        process = subprocess.Popen(['qstat', '-a'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Wait for the process to finish and get the output
        stdout, stderr = process.communicate()

        # Check if there was an error
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, process.args, output=stdout, stderr=stderr)

        # Return the standard output
        return stdout.decode()

    except subprocess.CalledProcessError as e:
        # Handle the error
        print(f"Error running subprocess: {e}")
        return None

if __name__ == "__main__":
    while True:
        output = run_subprocess()
        if output is not None:
            print(output)
        time.sleep(2)