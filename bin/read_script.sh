for (( i=512; i<=1048576; i*=2 ))
do
  NumReads=$(((209715200/$i)))
  echo "NumReads = $NumReads"
  $TACHYON_HOME/bin/succinct runTest ReadData /data $NumReads $i > log_$i
done
