@echo off

rem Check if pip is installed
python -m ensurepip

rem Install requirements
pip install -r requirements.txt

rem Run strategy.py
python main.py