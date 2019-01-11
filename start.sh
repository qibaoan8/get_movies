#!/bin/bash

source ~/pyenv/nlp/bin/activate

cd ~/git/get_movies/

date >> logs/start.log
python get_movies.py  >> logs/start.log 2>&1
