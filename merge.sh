#!/usr/bin/bash

set -e
FILE=$1

if [ -d "data/merged" ]; then
  rm -rf "data/merged"
fi

if [ -d "data/sorted" ]; then
  rm -rf "data/sorted"
fi

if [ -d "data/unsorted" ]; then
  rm -rf "data/unsorted"
fi

if [ -z "$FILE" ]; then
  echo "Usage: $0 <file>"
  exit 1
fi

if [ ! -f "$FILE" ]; then
  echo "File not found: $FILE"
  exit 1
fi

if [ ! -d "data" ]; then
  mkdir data
fi

if [ ! -d "data/unsorted" ]; then
  mkdir data/unsorted
fi

if [ ! -d "data/sorted" ]; then
  mkdir data/sorted
fi

if [ ! -d "data/merged" ]; then
  mkdir data/merged
fi

./build/DAM -s "$FILE" "data/unsorted"
sleep 1

./build/DAM -S "data/unsorted" "data/sorted"
sleep 1

./build/DAM -m "data/sorted" "data/merged"