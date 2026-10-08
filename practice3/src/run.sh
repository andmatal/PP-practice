#!/bin/sh
mkdir -p out
javac -encoding UTF-8 -d out src/Mandelbrot.java && java -Dfile.encoding=UTF-8 -cp out Mandelbrot
