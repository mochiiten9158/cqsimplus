Bash

#!/bin/bash

# Update the system
sudo apt update
sudo apt upgrade -y

# Add the deadsnakes PPA for Python 3.12
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update

# Install Python 3.12
sudo apt install python3.12 -y

# Install virtual environment tools
sudo apt install python3.12-venv -y

# Create a virtual environment
cd /local/repository && python3.12 -m venv .venv


