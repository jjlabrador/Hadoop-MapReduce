all:
	latex hadoop.tex
	latex hadoop.tex
	bibtex hadoop
	latex hadoop.tex
	dvips -Ppdf -G0 -o hadoop.ps hadoop.dvi
	ps2pdf -sPAPERSIZE=a4 -DMaxSubsetPct=100 -dCompatibilityLevel=1.2 -dSubsetFonts=true -dEmbedAllFonts=true hadoop.ps hadoop.pdf

clean:
	rm -f *.log *.dvi *.aux *.blg *.ps *.nav *.out *.snm *.toc *.bbl

eps:
	convert  img/versioncontrol1.png img/versioncontrol1.eps

