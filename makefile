distfile=dist/AWSLambdaRedshiftLoaderArtsy.zip
target:
	if [ -e $(distfile) ]; then rm ${distfile}; fi
	pushd source; zip -r ../${distfile} *; popd
