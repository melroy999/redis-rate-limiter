#!/bin/bash
echo "Removing old environment..."
rm -rf .venv

echo "Creating new virtual environment..."
python3 -m venv .venv

echo "Activating environment..."
source .venv/bin/activate

echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo "Setup complete. Run 'source .venv/bin/activate' to start."