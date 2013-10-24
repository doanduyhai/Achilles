#!/bin/bash

#first, clean up all existing files
rm $1/html/*.html 2>/dev/null
rm -rf $1/target/

echo ""
echo ""
echo "****************************************"
echo "*       Create target directory         *"
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
cp $1/html/achilles.css html/
cp $1/html/assets/* html/assets/

git clone https://github.com/doanduyhai/Achilles.wiki.git

#copy css to Achilles.wiki directory
cp $1/html/achilles.css Achilles.wiki/


#transform absolute URLs into relative
cd Achilles.wiki
sed -i -r 's/https:\/\/github.com\/doanduyhai\/Achilles\/wiki\/([^#)]+)/\.\/\1\.html/g' *.md
sed -i -r 's/https:\/\/github.com\/doanduyhai\/Achilles\/wiki/\.\/Home\.html/' _Sidebar.md
mv _Sidebar.md index.md

#add title to the index file
echo "# Achilles Documentation" > temp.md
echo " " >> temp.md
echo "### Table of contents" >> temp.md
echo " " >> temp.md
echo "<br/>" >> temp.md
cat index.md >> temp.md
mv temp.md index.md

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



