#!/bin/bash

#first, clean up all existing files
rm $1/html/*.html 2>/dev/null
rm -rf $1/target/

echo ""
echo ""
echo "****************************************"
echo "*      Create target directory         *"
echo "****************************************"
echo ""
echo ""
#create target directory
mkdir target

echo ""
echo ""
echo "****************************************"
echo "*        Cloning Achilles.wiki         *"
echo "****************************************"
echo ""
echo ""
#checkout the wiki source
cd target
mkdir html
mkdir html/assets
mkdir pdf
cp $1/html/achilles.css html/
cp $1/html/assets/* html/assets/

git clone https://github.com/doanduyhai/Achilles.wiki.git

#copy css to Achilles.wiki directory
cp $1/html/achilles.css Achilles.wiki/



cd Achilles.wiki

#rename Home to Presentation
mv Home.md Presentation.md

#add titles to each file
for i in `ls *.md`
do
title=`echo $i | sed 's/\.md//'`
echo "*** " > temp.md
echo "# $title" >> temp.md
echo "" >> temp.md
cat $i >> temp.md
mv temp.md $i
done

#add title to the index file
mv _Sidebar.md index.md
echo "# Achilles Documentation" >> temp.md
echo " " >> temp.md
echo "### Table of contents" >> temp.md
echo " " >> temp.md
echo "<br/>" >> temp.md
tail -n +3 index.md >> temp.md
mv temp.md index.md

#change initial PDF title
echo "# Achilles" > temp.md
tail -n +3 Presentation.md >> temp.md
mv temp.md Presentation.md



#transform absolute URLs into relative
sed -i -r 's/https:\/\/github.com\/doanduyhai\/Achilles\/wiki\/([^#)]+)/\.\/\1\.html/g' *.md
sed -i -r 's/https:\/\/github.com\/doanduyhai\/Achilles\/wiki/\.\/Presentation\.html/' index.md
#replace all URL from Home.html to Presentation.html
sed -i -r 's/Home\.html/Presentation\.html/g' *.md



echo ""
echo ""
echo "****************************************"
echo "*    Generating HTML documentation     *"
echo "****************************************"
echo ""
echo ""
#execute conversion using pandoc
find . -name \*.md -type f -exec pandoc -R -c achilles.css -f markdown_github+raw_html -t html5 -o $1/target/html/{}.html {} \;

#remove .md extension
cd $1/target/html
rename "s/\.md\.html/\.html/" *.html


echo ""
echo ""
echo "****************************************"
echo "*    Generating PDF documentation      *"
echo "****************************************"
echo ""
echo ""
cd $1/target/html
pandoc -f html -o $1/target/pdf/Achilles-documentation.pdf `grep -Po 'a href="\./[^.]+\.html' index.html | sed 's/a href="\.\///' | uniq`



