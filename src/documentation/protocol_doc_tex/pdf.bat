@echo off
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
bibtex -min-crossrefs -1 tmp/main
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
move tmp\main.pdf HyCube_protocol_documentation.pdf
