#!/bin/sh
pdfelatex -file-line-error-style tex/main.tex
pdfelatex -file-line-error-style tex/main.tex
bibtex -min-crossrefs -1 main
pdfelatex -file-line-error-style tex/main.tex
pdfelatex -file-line-error-style tex/main.tex
mv main.pdf HyCube_protocol_documentation.pdf
