#!/bin/bash

_cwd="$PWD"
_targetRoot="$HOME/Downloads"
_rootDirName="UMA_TELECOM"

rm -rf "$_targetRoot/$_rootDirName"
rm -rf "$_targetRoot/$_rootDirName.zip"
mkdir "$_targetRoot/$_rootDirName"
mkdir "$_targetRoot/$_rootDirName/in"
mkdir "$_targetRoot/$_rootDirName/out"
mkdir "$_targetRoot/$_rootDirName/tmp"
cp -r "$_cwd/src" "$_targetRoot/$_rootDirName/src"
cp -v "$_cwd/.env.example" "$_targetRoot/$_rootDirName/.env"
cp "$_cwd/app.py" "$_targetRoot/$_rootDirName/app.py"
cp "$_cwd/requirements.txt" "$_targetRoot/$_rootDirName/requirements.txt"
cp "$_cwd/test_data.json" "$_targetRoot/$_rootDirName/test_data.json"
find "$_targetRoot/$_rootDirName" | grep -E "(__pycache__|\.pyc|\.pyo$)" | xargs rm -rf
cd "$_targetRoot/${_rootDirName}"
zip -r "$_targetRoot/$_rootDirName.zip" .
