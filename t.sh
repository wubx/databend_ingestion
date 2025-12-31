for bt in  2000 3000 4000 5000 10000 20000 30000 40000 50000 100000 200000
do
	for type  in insert insert_no_presign streaming_load stage_load
	do
		echo $bt $type
		java -Dfile.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8 -Dsun.stderr.encoding=UTF-8 -jar ./db_ingestion-1.0-SNAPSHOT-shaded.jar  $type  2000000 $bt
	done
done

