#!/bin/bash

for ((j=0;j<300;j++));do
{
    /usr/bin/python3 /Users/zoe/Workspaces/github/faunadb/run.py -f ${j}
    wait
    /usr/bin/python3 /Users/zoe/Workspaces/github/faunadb/clean.py
}
done
