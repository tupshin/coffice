#!/bin/bash
npm ls -p | grep -v node_modules.*node_modules | awk -F/node_modules/ '{print $2}' | grep -vE '(npm|^$)' | xargs npm rm
npm install