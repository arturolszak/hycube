@echo off
rem wygenerowanie pierwszego pliku aux
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
rem tworzenie odwo³añ do bibliografii
bibtex -min-crossrefs -1 tmp/main
rem bibtex -min-crossrefs -1 st
rem bibtex -min-crossrefs -1 doc
rem bibtex -min-crossrefs -1 web
rem utworzenie indeksu
rem makeindex *.idx -o main.ind
rem aktualizacja aux
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
rem stworzenie poprawnych odnosników
pdflatex -file-line-error-style -output-directory=tmp -aux-directory=tmp -include-directory=tex tex/main.tex
rem i interaktywny pdf gotowy
move tmp\main.pdf HyCube_library_documentation.pdf
